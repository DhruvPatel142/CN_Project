#define _POSIX_C_SOURCE 200809L
#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#include <io.h>
#define PATH_CAP 4096
#define THREAD_RETURN DWORD WINAPI
#define THREAD_ARG LPVOID
#define THREAD_HANDLE HANDLE
#define MUTEX CRITICAL_SECTION
#define mutex_init(m) InitializeCriticalSection(m)
#define mutex_lock(m) EnterCriticalSection(m)
#define mutex_unlock(m) LeaveCriticalSection(m)
#define mutex_destroy(m) DeleteCriticalSection(m)
#define THREAD_EXIT return 0
#define sleep_ms(n) Sleep(n)
#else
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#define PATH_CAP PATH_MAX
#define THREAD_RETURN void *
#define THREAD_ARG void *
#define THREAD_HANDLE pthread_t
#define MUTEX pthread_mutex_t
#define mutex_init(m) pthread_mutex_init(m,NULL)
#define mutex_lock(m) pthread_mutex_lock(m)
#define mutex_unlock(m) pthread_mutex_unlock(m)
#define mutex_destroy(m) pthread_mutex_destroy(m)
#define THREAD_EXIT return NULL
#include <time.h>
static void sleep_ms(unsigned int n) { struct timespec ts; ts.tv_sec=n/1000; ts.tv_nsec=(long)(n%1000)*1000000L; nanosleep(&ts,NULL); }
#endif
#define CHECK_INTERVAL (64u*1024u)
#define MAX_WORKERS 64u
typedef struct { char name[FNAME_LEN]; uint64_t size, confirmed; int done; } file_t;
typedef struct { file_t *files; uint32_t count, next, session_id; MUTEX lock; char ip[256], topic[TOPIC_LEN], outdir[PATH_CAP]; uint16_t port; uint64_t txbase; volatile uint64_t useful, received, checkpoints, retries, reconnects; int failed; } app_t;
typedef struct { app_t *app; unsigned int id; } worker_arg_t;

static int field_string(const unsigned char *p,size_t n,char *out,size_t cap) {
    size_t len=0; while(len<n && p[len]) len++;
    if(!len || len>=cap || len==n) return -1;
    memcpy(out,p,len); out[len]=0; return 0;
}
static int parse_port(const char *s,uint16_t *out) {
    char *end=NULL; long x; errno=0; x=strtol(s,&end,10);
    if(errno || end==s || *end || x<1 || x>65535) return -1;
    *out=(uint16_t)x; return 0;
}
static int parse_count(const char *s,unsigned *out) {
    char *end=NULL; unsigned long x; errno=0; x=strtoul(s,&end,10);
    if(errno || end==s || *end || x<1 || x>MAX_WORKERS) return -1;
    *out=(unsigned)x; return 0;
}
static void close_socket(socket_t s) { if(s!=(socket_t)-1) CLOSESOCKET(s); }
static socket_t connect_server(const char *ip,uint16_t port) {
    socket_t s=socket(AF_INET,SOCK_STREAM,0); struct sockaddr_in a;
    if(s==(socket_t)-1) return (socket_t)-1;
    memset(&a,0,sizeof(a)); a.sin_family=AF_INET; a.sin_port=htons(port);
    if(inet_pton(AF_INET,ip,&a.sin_addr)!=1 || connect(s,(struct sockaddr *)&a,sizeof(a))==SOCKET_ERR) { close_socket(s); return (socket_t)-1; }
    return s;
}
static int mkdir_checked(const char *path) {
#ifdef _WIN32
    if(_mkdir(path)==0 || errno==EEXIST) return 0;
#else
    if(mkdir(path,0777)==0 || errno==EEXIST) return 0;
#endif
    return -1;
}
static int mkdir_tree(const char *path) {
    char copy[PATH_CAP]; size_t i,n=strlen(path);
    if(!n || n>=sizeof(copy)) return -1;
    strcpy(copy,path);
    for(i=1;i<n;i++) if(copy[i]=='/' || copy[i]=='\\') { char c=copy[i]; copy[i]=0; if(*copy && mkdir_checked(copy)) return -1; copy[i]=c; }
    return mkdir_checked(copy);
}
static int ensure_directory(const char *path) {
    if(mkdir_tree(path)==0) return 0;
#ifdef _WIN32
    { DWORD a=GetFileAttributesA(path); return a!=INVALID_FILE_ATTRIBUTES && (a&FILE_ATTRIBUTE_DIRECTORY)?0:-1; }
#else
    { struct stat st; return stat(path,&st)==0 && S_ISDIR(st.st_mode)?0:-1; }
#endif
}
static int truncate_file(FILE *fp,uint64_t offset) {
    if(fflush(fp)!=0) return -1;
#ifdef _WIN32
    return _chsize_s(_fileno(fp),offset)==0?0:-1;
#else
    return ftruncate(fileno(fp),(off_t)offset);
#endif
}
static int seek_file(FILE *fp,uint64_t offset) {
#ifdef _WIN32
    return _fseeki64(fp,(__int64)offset,SEEK_SET);
#else
    return fseeko(fp,(off_t)offset,SEEK_SET);
#endif
}
static int send_file_request(socket_t fd,app_t *a,uint32_t idx,uint64_t off,uint64_t tx) {
    unsigned char p[16]; put_u32(p,a->session_id); put_u32(p+4,idx); put_u64(p+8,off);
    return send_message(fd,"FILE_REQUEST",tx,p,sizeof(p));
}
static int file_path(app_t *a,const char *name,char *path,size_t cap,int part) {
    char n[FNAME_LEN+8]; if(!valid_component(name)) return -1;
    if(part) { if(snprintf(n,sizeof(n),"%s.part",name)>=(int)sizeof(n)) return -1; }
    else strcpy(n,name);
    return join_path(path,cap,a->outdir,n);
}
static int worker_transfer(app_t *a,unsigned worker,uint32_t idx,uint64_t *confirmed,socket_t *connection) {
    uint64_t tx=a->txbase+((uint64_t)idx<<16)+worker+1; unsigned attempts=0;
    for(;;) {
        socket_t fd=*connection; message_t m; char path[PATH_CAP]; FILE *out=NULL;
        if(fd==(socket_t)-1) { fd=connect_server(a->ip,a->port); *connection=fd; }
        if(fd==(socket_t)-1) goto retry;
        if(send_file_request(fd,a,idx,*confirmed,tx) || recv_message(fd,&m)) { close_socket(fd); *connection=(socket_t)-1; goto retry; }
        if(!strcmp(m.type,"ERROR")) { fprintf(stderr,"worker %u: server ERROR code=%u checkpoint=%llu\n",worker,m.payload_length>=10?get_u16(m.payload):0,(unsigned long long)(m.payload_length>=10?get_u64(m.payload+2):0)); free_message(&m); close_socket(fd); *connection=(socket_t)-1; return -1; }
        if(strcmp(m.type,"FILE") || m.transaction_id!=tx || m.payload_length!=4+8+8+FNAME_LEN || get_u32(m.payload)!=idx || get_u64(m.payload+12)!=*confirmed || field_string(m.payload+20,FNAME_LEN,path,sizeof(path))) { free_message(&m); close_socket(fd); *connection=(socket_t)-1; goto retry; }
        uint64_t size=get_u64(m.payload+4), start=get_u64(m.payload+12); char filename[FNAME_LEN]; strcpy(filename,path); free_message(&m);
        if(size!=a->files[idx].size || start>size || file_path(a,filename,path,sizeof(path),1)) { close_socket(fd); *connection=(socket_t)-1; return -1; }
        out=fopen(path,start?"rb+":"wb"); if(!out && start==0) out=fopen(path,"wb");
        if(!out) { close_socket(fd); *connection=(socket_t)-1; return -1; }
        if(truncate_file(out,start) || seek_file(out,start)) { fclose(out); close_socket(fd); *connection=(socket_t)-1; return -1; }
        uint64_t offset=start,since=0; unsigned char buf[IO_BUFFER_SIZE]; int failed=0;
        while(offset<size) {
            size_t want=(size-offset>sizeof(buf))?sizeof(buf):(size_t)(size-offset);
            if(recv_all(fd,buf,want) || fwrite(buf,1,want,out)!=want) { failed=1; break; }
            offset+=want; since+=want;
            mutex_lock(&a->lock); a->received+=want; mutex_unlock(&a->lock);
            if(since>=CHECK_INTERVAL || offset==size) {
                /* On reconnect, bytes beyond this flushed checkpoint are discarded locally. */
                if(fflush(out)!=0) { failed=1; break; }
                unsigned char cp[16]; put_u32(cp,a->session_id); put_u32(cp+4,idx); put_u64(cp+8,offset);
                if(send_message(fd,"CHECKPOINT",tx,cp,sizeof(cp))) { failed=1; break; }
                *confirmed=offset; mutex_lock(&a->lock); a->checkpoints++; mutex_unlock(&a->lock); since=0;
            }
        }
        if(failed) {
            truncate_file(out,*confirmed); fclose(out); close_socket(fd); *connection=(socket_t)-1;
            mutex_lock(&a->lock); a->retries++; mutex_unlock(&a->lock); goto retry;
        }
        unsigned char done[8]; put_u32(done,a->session_id); put_u32(done+4,idx);
        if(send_message(fd,"FILE_DONE",tx,done,sizeof(done)) || recv_message(fd,&m)) {
            truncate_file(out,*confirmed); fclose(out); close_socket(fd); *connection=(socket_t)-1; mutex_lock(&a->lock); a->retries++; mutex_unlock(&a->lock); goto retry;
        }
        int valid=!strcmp(m.type,"FILE_DONE") && m.transaction_id==tx && m.payload_length==8 && get_u32(m.payload)==a->session_id && get_u32(m.payload+4)==idx;
        free_message(&m); fclose(out);
        if(!valid) { close_socket(fd); *connection=(socket_t)-1; goto retry; }
        char final[PATH_CAP]; if(file_path(a,filename,final,sizeof(final),0)) { close_socket(fd); *connection=(socket_t)-1; return -1; }
#ifdef _WIN32
        remove(final);
#endif
        if(rename(path,final)!=0) { close_socket(fd); *connection=(socket_t)-1; return -1; }
        *confirmed=size; mutex_lock(&a->lock); a->useful+=size; a->files[idx].done=1; a->files[idx].confirmed=size; mutex_unlock(&a->lock);
        printf("FILE_DONE worker=%u index=%u file=%s bytes=%llu\n",worker,idx,filename,(unsigned long long)size); fflush(stdout);
        return 0;
retry:
        attempts++; mutex_lock(&a->lock); a->reconnects++; mutex_unlock(&a->lock);
        if(attempts>10000) { fprintf(stderr,"worker %u exhausted retries for file index %u\n",worker,idx); return -1; }
        sleep_ms(attempts<8?attempts*100:800);
    }
}
static THREAD_RETURN worker_main(THREAD_ARG arg) {
    worker_arg_t *w=(worker_arg_t *)arg; app_t *a=w->app; unsigned id=w->id; socket_t connection=(socket_t)-1;
    for(;;) {
        uint32_t idx;
        /* Claim a whole file under the mutex; each worker writes a distinct output file. */
        mutex_lock(&a->lock); if(a->failed || a->next>=a->count) { mutex_unlock(&a->lock); break; } idx=a->next++; mutex_unlock(&a->lock);
        uint64_t confirmed=a->files[idx].confirmed;
        if(worker_transfer(a,id,idx,&confirmed,&connection)!=0) { mutex_lock(&a->lock); a->failed=1; mutex_unlock(&a->lock); break; }
    }
    close_socket(connection); free(w); THREAD_EXIT;
}
static int control_get(app_t *a) {
    socket_t fd=connect_server(a->ip,a->port); unsigned char payload[TOPIC_LEN]; message_t m; uint64_t tx=1;
    if(fd==(socket_t)-1) { report_socket_error("control connect"); return -1; }
    size_t tl=strlen(a->topic); memcpy(payload,a->topic,tl);
    if(send_message(fd,"GET",tx,payload,(uint32_t)tl) || recv_message(fd,&m)) { close_socket(fd); return -1; }
    if(!strcmp(m.type,"ERROR")) { fprintf(stderr,"server rejected topic (error %u)\n",m.payload_length>=2?get_u16(m.payload):0); free_message(&m); close_socket(fd); return -1; }
    if(strcmp(m.type,"SESSION") || m.transaction_id!=tx || m.payload_length!=8) { free_message(&m); close_socket(fd); return -1; }
    a->session_id=get_u32(m.payload); a->count=get_u32(m.payload+4); free_message(&m);
    if(a->count>1000000u) { close_socket(fd); return -1; }
    a->files=(file_t *)calloc(a->count?a->count:1,sizeof(file_t)); if(!a->files) { close_socket(fd); return -1; }
    for(uint32_t i=0;i<a->count;i++) {
        if(recv_message(fd,&m)) { close_socket(fd); return -1; }
        if(strcmp(m.type,"FILELIST_ENTRY") || m.transaction_id!=tx || m.payload_length!=4+8+FNAME_LEN || get_u32(m.payload)!=i || field_string(m.payload+12,FNAME_LEN,a->files[i].name,sizeof(a->files[i].name))) { free_message(&m); close_socket(fd); return -1; }
        a->files[i].size=get_u64(m.payload+4); free_message(&m);
    }
    if(recv_message(fd,&m)) { close_socket(fd); return -1; }
    int ok=!strcmp(m.type,"FILELIST_END") && m.transaction_id==tx && m.payload_length==0; free_message(&m); close_socket(fd); return ok?0:-1;
}
static int session_done(app_t *a) {
    socket_t fd=connect_server(a->ip,a->port); unsigned char p[4]; if(fd==(socket_t)-1) return -1; put_u32(p,a->session_id);
    int r=send_message(fd,"SESSION_DONE",2,p,sizeof(p)); close_socket(fd); return r;
}
int main(int argc,char **argv) {
    app_t a; uint16_t port; unsigned workers=4; char topic_dir[PATH_CAP]; double start,elapsed; THREAD_HANDLE *threads=NULL;
    if((argc!=5 && argc!=6) || parse_port(argv[2],&port) || (argc==6 && parse_count(argv[5],&workers)) || strlen(argv[1])>=sizeof(a.ip) || strlen(argv[3])>=TOPIC_LEN || !valid_component(argv[3]) || strlen(argv[4])>=PATH_CAP) {
        fprintf(stderr,"Usage: %s <server_ip> <server_port> <topic> <output_dir> [num_connections 1..64]\n",argv[0]); return 2;
    }
    memset(&a,0,sizeof(a)); strcpy(a.ip,argv[1]); strcpy(a.topic,argv[3]); strcpy(a.outdir,argv[4]); a.port=port; a.txbase=((uint64_t)time(NULL)<<24)^(uintptr_t)&a; if(!a.txbase) a.txbase=3;
    if(socket_runtime_init()) { fprintf(stderr,"socket runtime initialization failed\n"); return 1; }
    if(control_get(&a)!=0) { fprintf(stderr,"failed to obtain a valid topic file list\n"); socket_runtime_cleanup(); free(a.files); return 1; }
    if(join_path(topic_dir,sizeof(topic_dir),a.outdir,a.topic) || ensure_directory(topic_dir)!=0) { fprintf(stderr,"cannot create output topic directory\n"); free(a.files); socket_runtime_cleanup(); return 1; }
    strcpy(a.outdir,topic_dir); mutex_init(&a.lock); if(workers>a.count && a.count) workers=a.count;
    printf("SESSION topic=%s session_id=%u files=%u workers=%u default_workers=4\n",a.topic,a.session_id,a.count,workers); fflush(stdout);
    start=monotonic_seconds();
    if(a.count) {
        threads=(THREAD_HANDLE *)calloc(workers,sizeof(*threads)); if(!threads) { free(a.files); return 1; }
        unsigned created=0;
        for(;created<workers;created++) {
            worker_arg_t *w=(worker_arg_t *)malloc(sizeof(*w)); if(!w) break; w->app=&a; w->id=created+1;
#ifdef _WIN32
            threads[created]=CreateThread(NULL,0,worker_main,w,0,NULL); if(!threads[created]) { free(w); break; }
#else
            if(pthread_create(&threads[created],NULL,worker_main,w)!=0) { free(w); break; }
#endif
        }
        if(created<workers) { mutex_lock(&a.lock); a.failed=1; mutex_unlock(&a.lock); }
        for(unsigned i=0;i<created;i++) {
#ifdef _WIN32
            WaitForSingleObject(threads[i],INFINITE); CloseHandle(threads[i]);
#else
            pthread_join(threads[i],NULL);
#endif
        }
        free(threads);
    }
    elapsed=monotonic_seconds()-start;
    int failed=a.failed; uint32_t n=a.count;
    if(!failed && session_done(&a)!=0) { fprintf(stderr,"warning: SESSION_DONE could not be delivered; server will expire session\n"); }
    double rate=elapsed>0?(double)a.useful/elapsed:0.0;
    uint64_t redundant=a.received>a.useful?a.received-a.useful:0;
    printf("RESULT topic=%s files=%u workers=%u total_file_bytes_received=%llu useful_bytes=%llu redundant_bytes=%llu checkpoints=%llu checkpoint_overhead_bytes=%llu retries=%llu reconnects=%llu seconds=%.6f throughput_Bps=%.2f status=%s\n",a.topic,n,workers,(unsigned long long)a.received,(unsigned long long)a.useful,(unsigned long long)redundant,(unsigned long long)a.checkpoints,(unsigned long long)(a.checkpoints*44u),(unsigned long long)a.retries,(unsigned long long)a.reconnects,elapsed,rate,failed?"FAILED":"COMPLETE");
    mutex_destroy(&a.lock); free(a.files); socket_runtime_cleanup(); return failed?1:0;
}
