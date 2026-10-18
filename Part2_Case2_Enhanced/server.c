#define _POSIX_C_SOURCE 200809L
#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <time.h>
#include <math.h>
#include <ctype.h>
#include <sys/stat.h>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
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
#else
#include <dirent.h>
#include <pthread.h>
#include <unistd.h>
#include <limits.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#define PATH_CAP PATH_MAX
#define THREAD_RETURN void *
#define THREAD_ARG void *
#define THREAD_HANDLE pthread_t
#define MUTEX pthread_mutex_t
#define mutex_init(m) pthread_mutex_init(m, NULL)
#define mutex_lock(m) pthread_mutex_lock(m)
#define mutex_unlock(m) pthread_mutex_unlock(m)
#define mutex_destroy(m) pthread_mutex_destroy(m)
#define THREAD_EXIT return NULL
#endif

#define MAX_SESSIONS 128
#define CHECK_INTERVAL (64u * 1024u)
#define SESSION_TTL 3600.0

typedef struct { char name[FNAME_LEN]; uint64_t size, offset, owner; int complete; } file_state_t;
typedef struct { int active; uint32_t id; char topic[TOPIC_LEN], directory[PATH_CAP]; file_state_t *files; uint32_t count; double touched; } session_t;
typedef struct { socket_t fd; uint64_t id; } conn_arg_t;
static session_t sessions[MAX_SESSIONS];
static MUTEX sessions_mutex;
static double failure_probability;
static char topics_root[PATH_CAP];
static volatile uint64_t next_conn_id = 1, fault_count = 0;
static uint32_t next_session_id = 1000;

static void close_socket(socket_t s) { if (s != (socket_t)-1) CLOSESOCKET(s); }
static uint64_t atomic_increment(volatile uint64_t *p) {
#ifdef _WIN32
    return (uint64_t)InterlockedIncrement64((volatile LONG64 *)p);
#else
    return __sync_add_and_fetch(p, 1);
#endif
}
static int probability_hit(uint64_t *rng) {
    *rng ^= *rng << 13; *rng ^= *rng >> 7; *rng ^= *rng << 17;
    return (double)(*rng >> 11) * (1.0 / 9007199254740992.0) < failure_probability;
}
static int parse_probability(const char *s, double *out) {
    char *end = NULL; double v; errno = 0; v = strtod(s, &end);
    if (!s[0] || isspace((unsigned char)s[0]) || errno || end == s || *end || !isfinite(v) || v < 0.0 || v > 1.0) return -1;
    *out = v; return 0;
}
static int root_is_directory(const char *path) {
#ifdef _WIN32
    DWORD a=GetFileAttributesA(path);
    return a!=INVALID_FILE_ATTRIBUTES && (a&FILE_ATTRIBUTE_DIRECTORY)!=0;
#else
    struct stat st;
    return stat(path,&st)==0 && S_ISDIR(st.st_mode);
#endif
}
static int parse_port(const char *s, unsigned short *out) {
    char *end = NULL; long v; errno = 0; v = strtol(s, &end, 10);
    if (errno || end == s || *end || v < 1 || v > 65535) return -1;
    *out = (unsigned short)v; return 0;
}
static int valid_name(const char *s) { return valid_component(s) && strlen(s) < FNAME_LEN; }
static int regular_file(const char *path, uint64_t *size) {
#ifdef _WIN32
    struct _stat64 st; if (_stat64(path, &st) || (st.st_mode & _S_IFREG) == 0 || st.st_size < 0) return 0;
#else
    struct stat st; if (stat(path, &st) || !S_ISREG(st.st_mode) || st.st_size < 0) return 0;
#endif
    *size = (uint64_t)st.st_size; return 1;
}
static int cmp_names(const void *a, const void *b) { return strcmp(((const file_state_t *)a)->name, ((const file_state_t *)b)->name); }
static int enumerate(session_t *s) {
#ifdef _WIN32
    char pattern[PATH_CAP]; WIN32_FIND_DATAA ent; HANDLE h;
    if (join_path(pattern, sizeof(pattern), s->directory, "*") != 0) return -1;
    h = FindFirstFileA(pattern, &ent); if (h == INVALID_HANDLE_VALUE) return GetLastError() == ERROR_FILE_NOT_FOUND ? 0 : -1;
    do {
        if (!(ent.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) && valid_name(ent.cFileName)) {
            file_state_t *n = (file_state_t *)realloc(s->files, (s->count + 1u) * sizeof(*n));
            if (!n) { FindClose(h); return -1; }
            s->files = n; memset(&s->files[s->count], 0, sizeof(*n)); strcpy(s->files[s->count].name, ent.cFileName);
            s->files[s->count].size = ((uint64_t)ent.nFileSizeHigh << 32) | ent.nFileSizeLow; s->count++;
        }
    } while (FindNextFileA(h, &ent));
    FindClose(h);
#else
    DIR *d = opendir(s->directory); struct dirent *ent;
    if (!d) return -1;
    while ((ent = readdir(d)) != NULL) {
        char path[PATH_CAP]; uint64_t size; file_state_t *n;
        if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..") || !valid_name(ent->d_name)) continue;
        if (join_path(path, sizeof(path), s->directory, ent->d_name) || !regular_file(path, &size)) continue;
        n = (file_state_t *)realloc(s->files, (s->count + 1u) * sizeof(*n));
        if (!n) { closedir(d); return -1; }
        s->files = n; memset(&s->files[s->count], 0, sizeof(*n)); strcpy(s->files[s->count].name, ent->d_name); s->files[s->count].size = size; s->count++;
    }
    closedir(d);
#endif
    if (s->count > 1) qsort(s->files, s->count, sizeof(*s->files), cmp_names);
    return 0;
}
static int session_has_workers(const session_t *s) {
    uint32_t i;
    for (i=0;i<s->count;i++) if (s->files[i].owner) return 1;
    return 0;
}
static void reap_expired_sessions(void) {
    size_t i; double now=monotonic_seconds();
    mutex_lock(&sessions_mutex);
    for(i=0;i<MAX_SESSIONS;i++) {
        session_t *s=&sessions[i];
        if(s->active && now-s->touched>SESSION_TTL && !session_has_workers(s)) {
            fprintf(stderr,"SESSION_EXPIRED session=%u topic=%s\n",s->id,s->topic); fflush(stderr);
            free(s->files); memset(s,0,sizeof(*s));
        }
    }
    mutex_unlock(&sessions_mutex);
}
static session_t *lookup_session(uint32_t id) {
    size_t i; double now = monotonic_seconds();
    for (i = 0; i < MAX_SESSIONS; i++) {
        if (sessions[i].active && now - sessions[i].touched > SESSION_TTL && !session_has_workers(&sessions[i])) {
            fprintf(stderr,"SESSION_EXPIRED session=%u topic=%s\n",sessions[i].id,sessions[i].topic); fflush(stderr);
            free(sessions[i].files); memset(&sessions[i], 0, sizeof(sessions[i]));
        }
        if (sessions[i].active && sessions[i].id == id) { sessions[i].touched = now; return &sessions[i]; }
    }
    return NULL;
}
static THREAD_RETURN session_reaper(THREAD_ARG unused) {
    (void)unused;
    for(;;) {
#ifdef _WIN32
        Sleep(60000);
#else
        struct timespec ts; ts.tv_sec=60; ts.tv_nsec=0; nanosleep(&ts,NULL);
#endif
        reap_expired_sessions();
    }
    THREAD_EXIT;
}
static void free_session(session_t *s) { free(s->files); memset(s, 0, sizeof(*s)); }
static int send_error(socket_t fd, uint64_t tx, uint16_t code, uint64_t current) {
    unsigned char p[10]; put_u16(p, code); put_u64(p + 2, current); return send_message(fd, "ERROR", tx, p, sizeof(p));
}
static int topic_path(const char *root, const char *topic, char *out, size_t cap) {
    char p[PATH_CAP]; if (!valid_component(topic) || join_path(p, sizeof(p), root, topic)) return -1;
#ifdef _WIN32
    { DWORD a = GetFileAttributesA(p); if (a == INVALID_FILE_ATTRIBUTES || !(a & FILE_ATTRIBUTE_DIRECTORY)) return -1; }
#else
    { struct stat st; if (stat(p, &st) || !S_ISDIR(st.st_mode)) return -1; }
#endif
    if (strlen(p) >= cap) return -1;
    strcpy(out, p);
    return 0;
}
static int send_control_session(socket_t fd, const char *topic, const char *dir, uint64_t tx) {
    session_t tmp; size_t slot; uint32_t i; unsigned char p[8]; memset(&tmp, 0, sizeof(tmp));
    strcpy(tmp.topic, topic); strcpy(tmp.directory, dir);
    if (enumerate(&tmp) != 0) { free(tmp.files); return send_error(fd, tx, 4, 0); }
    mutex_lock(&sessions_mutex);
    for (slot = 0; slot < MAX_SESSIONS && sessions[slot].active; slot++) {}
    if (slot == MAX_SESSIONS) { mutex_unlock(&sessions_mutex); free(tmp.files); return send_error(fd,tx,5,0); }
    if (++next_session_id == 0) ++next_session_id;
    tmp.id = next_session_id; tmp.active = 1; tmp.touched = monotonic_seconds(); sessions[slot] = tmp;
    fprintf(stderr,"SESSION_CREATED session=%u topic=%s files=%u\n",tmp.id,tmp.topic,tmp.count); fflush(stderr);
    mutex_unlock(&sessions_mutex);
    put_u32(p, tmp.id); put_u32(p + 4, tmp.count);
    if (send_message(fd, "SESSION", tx, p, 8)) return -1;
    for (i = 0; i < tmp.count; i++) {
        unsigned char entry[4 + 8 + FNAME_LEN]; memset(entry, 0, sizeof(entry));
        put_u32(entry, i); put_u64(entry + 4, tmp.files[i].size); memcpy(entry + 12, tmp.files[i].name, strlen(tmp.files[i].name));
        if (send_message(fd, "FILELIST_ENTRY", tx, entry, sizeof(entry))) return -1;
    }
    return send_message(fd, "FILELIST_END", tx, NULL, 0);
}
static int handle_control(socket_t fd, const message_t *m) {
    char topic[TOPIC_LEN], dir[PATH_CAP]; uint32_t sid;
    if (!strcmp(m->type, "GET")) {
        if (!m->payload_length || m->payload_length >= sizeof(topic))
            return send_error(fd,m->transaction_id,1,0);
        memcpy(topic,m->payload,m->payload_length); topic[m->payload_length]=0;
        if (topic_path(topics_root,topic,dir,sizeof(dir)))
            return send_error(fd,m->transaction_id,2,0);
        return send_control_session(fd,topic,dir,m->transaction_id);
    }
    if (!strcmp(m->type, "SESSION_DONE")) {
        if (m->payload_length != 4) return send_error(fd,m->transaction_id,1,0);
        sid=get_u32(m->payload); mutex_lock(&sessions_mutex); session_t *s=lookup_session(sid);
        if (s) { fprintf(stderr,"SESSION_DONE session=%u\n",sid); fflush(stderr); free_session(s); }
        else { fprintf(stderr,"SESSION_DONE_UNKNOWN session=%u\n",sid); fflush(stderr); }
        mutex_unlock(&sessions_mutex);
        return 0;
    }
    return send_error(fd,m->transaction_id,1,0);
}
static void release_owner(uint32_t sid,uint32_t idx,uint64_t owner,int done) {
    mutex_lock(&sessions_mutex); session_t *s=lookup_session(sid);
    if (s && idx<s->count && s->files[idx].owner==owner) {
        s->files[idx].owner=0; if (done) { s->files[idx].complete=1; s->files[idx].offset=s->files[idx].size; }
    }
    mutex_unlock(&sessions_mutex);
}
static int send_file_metadata(socket_t fd,uint32_t idx,const file_state_t *f,uint64_t off,uint64_t tx) {
    unsigned char p[4+8+8+FNAME_LEN]; memset(p,0,sizeof(p)); put_u32(p,idx); put_u64(p+4,f->size); put_u64(p+12,off); memcpy(p+20,f->name,strlen(f->name));
    return send_message(fd,"FILE",tx,p,sizeof(p));
}
static int handle_worker(socket_t fd,uint64_t owner,uint64_t *rng,const message_t *m) {
    uint32_t sid,index; uint64_t requested,size,start,tx=m->transaction_id,offset,since=0; session_t *s; file_state_t *f; char path[PATH_CAP]; FILE *fp; unsigned char buf[IO_BUFFER_SIZE];
    if (strcmp(m->type,"FILE_REQUEST") || m->payload_length!=16) return send_error(fd,tx,1,0);
    sid=get_u32(m->payload); index=get_u32(m->payload+4); requested=get_u64(m->payload+8);
    mutex_lock(&sessions_mutex); s=lookup_session(sid);
    if (!s || index>=s->count) { mutex_unlock(&sessions_mutex); return send_error(fd,tx,2,0); }
    f=&s->files[index];
    if ((f->complete && requested != f->size) || f->owner || requested>f->offset || requested>f->size) {
        uint64_t cp=f->offset; mutex_unlock(&sessions_mutex); return send_error(fd,tx,3,cp);
    }
    f->owner=owner; f->offset=requested; s->touched=monotonic_seconds(); size=f->size; start=requested;
    if (join_path(path,sizeof(path),s->directory,f->name)) { f->owner=0; mutex_unlock(&sessions_mutex); return send_error(fd,tx,4,start); }
    char filename[FNAME_LEN]; file_state_t snapshot=*f; strcpy(filename,f->name);
    mutex_unlock(&sessions_mutex);
    fp=fopen(path,"rb"); if (!fp) { release_owner(sid,index,owner,0); return send_error(fd,tx,4,start); }
#ifdef _WIN32
    struct _stat64 file_stat;
    if (_fstat64(_fileno(fp),&file_stat) || (file_stat.st_mode & _S_IFREG)==0 || file_stat.st_size<0 || (uint64_t)file_stat.st_size!=snapshot.size) {
        fclose(fp); release_owner(sid,index,owner,0); return send_error(fd,tx,4,start);
    }
    if (_fseeki64(fp,(__int64)start,SEEK_SET)) { fclose(fp); release_owner(sid,index,owner,0); return send_error(fd,tx,4,start); }
#else
    struct stat file_stat;
    if (fstat(fileno(fp),&file_stat) || !S_ISREG(file_stat.st_mode) || file_stat.st_size<0 || (uint64_t)file_stat.st_size!=snapshot.size) {
        fclose(fp); release_owner(sid,index,owner,0); return send_error(fd,tx,4,start);
    }
    if (fseeko(fp,(off_t)start,SEEK_SET)) { fclose(fp); release_owner(sid,index,owner,0); return send_error(fd,tx,4,start); }
#endif
    if (send_file_metadata(fd,index,&snapshot,start,tx)) { fclose(fp); release_owner(sid,index,owner,0); return -1; }
    offset=start;
    while (offset<size) {
        size_t want=(size-offset>sizeof(buf))?sizeof(buf):(size_t)(size-offset), n=fread(buf,1,want,fp);
        if (!n || send_all(fd,buf,n)) { fclose(fp); release_owner(sid,index,owner,0); return -1; }
        offset+=n; since+=n;
        if (since>=CHECK_INTERVAL || offset==size) {
            message_t cp; if (recv_message(fd,&cp)) { fclose(fp); release_owner(sid,index,owner,0); return -1; }
            int ok=!strcmp(cp.type,"CHECKPOINT") && cp.transaction_id==tx && cp.payload_length==16 && get_u32(cp.payload)==sid && get_u32(cp.payload+4)==index && get_u64(cp.payload+8)==offset;
            free_message(&cp);
            if (!ok) { fclose(fp); release_owner(sid,index,owner,0); return send_error(fd,tx,3,offset); }
            mutex_lock(&sessions_mutex); s=lookup_session(sid);
            if (!s || index>=s->count || s->files[index].owner!=owner) { mutex_unlock(&sessions_mutex); fclose(fp); return send_error(fd,tx,2,offset); }
            s->files[index].offset=offset; s->touched=monotonic_seconds(); mutex_unlock(&sessions_mutex);
            int interval_due = since >= CHECK_INTERVAL;
            since=0;
            if (interval_due && probability_hit(rng)) {
                uint64_t ev=atomic_increment(&fault_count);
                fprintf(stderr,"FAULT_INJECTED event=%llu session=%u file_index=%u file=%s file_offset=%llu probability=%.6f interval=%u\n",(unsigned long long)ev,sid,index,filename,(unsigned long long)offset,failure_probability,CHECK_INTERVAL); fflush(stderr);
                fclose(fp); release_owner(sid,index,owner,0); return -1;
            }
        }
    }
    fclose(fp); message_t done; if (recv_message(fd,&done)) { release_owner(sid,index,owner,0); return -1; }
    int ok=!strcmp(done.type,"FILE_DONE") && done.transaction_id==tx && done.payload_length==8 && get_u32(done.payload)==sid && get_u32(done.payload+4)==index;
    free_message(&done); if (!ok) { release_owner(sid,index,owner,0); return send_error(fd,tx,3,size); }
    unsigned char ack[8]; put_u32(ack,sid); put_u32(ack+4,index);
    release_owner(sid,index,owner,1); return send_message(fd,"FILE_DONE",tx,ack,sizeof(ack));
}
static THREAD_RETURN connection_thread(THREAD_ARG v) {
    conn_arg_t *a=(conn_arg_t *)v; socket_t fd=a->fd; uint64_t owner=a->id;
    uint64_t rng=(uint64_t)time(NULL)^(uintptr_t)owner^(uintptr_t)&fd^(uint64_t)clock(); if(!rng) rng=UINT64_C(0x9e3779b97f4a7c15);
    for (;;) {
        message_t m; if(recv_message(fd,&m)) break; int r;
        if(!strcmp(m.type,"GET") || !strcmp(m.type,"SESSION_DONE")) r=handle_control(fd,&m);
        else if(!strcmp(m.type,"FILE_REQUEST")) r=handle_worker(fd,owner,&rng,&m);
        else r=send_error(fd,m.transaction_id,1,0);
        free_message(&m); if(r) break;
    }
    close_socket(fd); free(a); THREAD_EXIT;
}
static socket_t create_listener(unsigned short port) {
    socket_t s=socket(AF_INET,SOCK_STREAM,0); struct sockaddr_in a; int yes=1;
    if(s==(socket_t)-1) return (socket_t)-1;
    setsockopt(s,SOL_SOCKET,SO_REUSEADDR,(const char *)&yes,sizeof(yes));
    memset(&a,0,sizeof(a)); a.sin_family=AF_INET; a.sin_addr.s_addr=htonl(INADDR_ANY); a.sin_port=htons(port);
    if(bind(s,(struct sockaddr *)&a,sizeof(a))==SOCKET_ERR || listen(s,128)==SOCKET_ERR) { report_socket_error("bind/listen"); close_socket(s); return (socket_t)-1; }
    return s;
}
int main(int argc,char **argv) {
    unsigned short port; socket_t listener;
    if(argc!=4 || parse_port(argv[1],&port) || parse_probability(argv[3],&failure_probability)) { fprintf(stderr,"Usage: %s <port> <topics_root_dir> <failure_probability 0.0..1.0>\n",argv[0]); return 2; }
    if(socket_runtime_init()) { fprintf(stderr,"socket runtime initialization failed\n"); return 1; }
    if (strlen(argv[2]) >= sizeof(topics_root)) { fprintf(stderr,"topics root path too long\n"); socket_runtime_cleanup(); return 2; }
    strcpy(topics_root,argv[2]);
    if(!root_is_directory(topics_root)) { fprintf(stderr,"topics root is not an accessible directory: %s\n",topics_root); socket_runtime_cleanup(); return 2; }
    mutex_init(&sessions_mutex); listener=create_listener(port); if(listener==(socket_t)-1) { mutex_destroy(&sessions_mutex); socket_runtime_cleanup(); return 1; }
    printf("CONFIG case=2-enhanced port=%u topics_root=%s failure_probability=%.6f check_interval_bytes=%u default_workers=4 actual_injected_failures=0\n",port,argv[2],failure_probability,CHECK_INTERVAL); fflush(stdout);
    THREAD_HANDLE reaper;
#ifdef _WIN32
    reaper=CreateThread(NULL,0,session_reaper,NULL,0,NULL);
    if(reaper) CloseHandle(reaper);
#else
    if(pthread_create(&reaper,NULL,session_reaper,NULL)==0) pthread_detach(reaper);
#endif
    for (;;) {
        struct sockaddr_in peer;
#ifdef _WIN32
        int plen=sizeof(peer);
#else
        socklen_t plen=sizeof(peer);
#endif
        socket_t fd=accept(listener,(struct sockaddr *)&peer,&plen); if(fd==(socket_t)-1) continue;
        conn_arg_t *a=(conn_arg_t *)malloc(sizeof(*a)); if(!a) { close_socket(fd); continue; }
        a->fd=fd; a->id=atomic_increment(&next_conn_id); THREAD_HANDLE th;
#ifdef _WIN32
        th=CreateThread(NULL,0,connection_thread,a,0,NULL); if(!th) { close_socket(fd); free(a); continue; } CloseHandle(th);
#else
        if(pthread_create(&th,NULL,connection_thread,a)!=0) { close_socket(fd); free(a); continue; } pthread_detach(th);
#endif
    }
}
