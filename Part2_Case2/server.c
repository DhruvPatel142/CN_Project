#define _POSIX_C_SOURCE 200809L
#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <ctype.h>
#include <math.h>
#include <stdatomic.h>
#include <time.h>
#include <sys/stat.h>

#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#define PATH_CAP 4096u
typedef HANDLE worker_t;
#else
#include <dirent.h>
#include <pthread.h>
#include <limits.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#define PATH_CAP ((size_t)PATH_MAX)
typedef pthread_t worker_t;
#endif

typedef struct { char *name; char *path; } topic_t;
typedef struct { char *name; uint64_t size; } file_record;
typedef struct { file_record *items; size_t count; } file_list;
typedef struct { socket_t fd; const topic_t *topics; size_t topic_count; double probability; uint64_t seed; } worker_arg;
typedef struct {
    int active;
    uint32_t id;
    char topic[TOPIC_LEN];
    file_list files;
    unsigned char *completed;
    double last_activity;
} session_t;

#define MAX_SESSIONS 256

static topic_t *topics;
static size_t topic_count;
static atomic_uint_fast64_t failures_injected;
static atomic_uint_fast64_t seed_counter = 1;
static session_t sessions[MAX_SESSIONS];
static uint32_t next_session_id = 1;
#ifdef _WIN32
static CRITICAL_SECTION session_mutex;
static void lock_sessions(void) { EnterCriticalSection(&session_mutex); }
static void unlock_sessions(void) { LeaveCriticalSection(&session_mutex); }
#else
static pthread_mutex_t session_mutex = PTHREAD_MUTEX_INITIALIZER;
static void lock_sessions(void) { (void)pthread_mutex_lock(&session_mutex); }
static void unlock_sessions(void) { (void)pthread_mutex_unlock(&session_mutex); }
#endif

static void free_files(file_list *list);

static void release_session_contents(session_t *session) {
    free_files(&session->files);
    free(session->completed);
    session->completed = NULL;
}

static session_t *create_session(const char *topic, file_list *files) {
    size_t i, slot = MAX_SESSIONS;
    double now = monotonic_seconds();
    session_t *session;
    lock_sessions();
    for (i = 0; i < MAX_SESSIONS; ++i) {
        if (sessions[i].active && now - sessions[i].last_activity > 3600.0) sessions[i].active = 0;
        if (!sessions[i].active && slot == MAX_SESSIONS) slot = i;
    }
    if (slot == MAX_SESSIONS) { unlock_sessions(); return NULL; }
    session = &sessions[slot];
    release_session_contents(session);
    if (files->count) {
        session->completed = (unsigned char *)calloc(files->count, sizeof(*session->completed));
        if (!session->completed) { unlock_sessions(); return NULL; }
    }
    session->active = 1;
    session->id = next_session_id++;
    if (session->id == 0) session->id = next_session_id++;
    (void)set_field(session->topic, sizeof(session->topic), topic);
    session->files = *files;
    memset(files, 0, sizeof(*files));
    session->last_activity = now;
    unlock_sessions();
    fprintf(stderr, "SESSION_CREATED id=%u topic=%s files=%lu\n", session->id, session->topic, (unsigned long)session->files.count);
    return session;
}

static session_t *find_session(uint32_t id, const char *topic) {
    size_t i;
    session_t *found = NULL;
    lock_sessions();
    for (i = 0; i < MAX_SESSIONS; ++i) {
        if (sessions[i].active && sessions[i].id == id && !strcmp(sessions[i].topic, topic)) {
            sessions[i].last_activity = monotonic_seconds();
            found = &sessions[i];
            break;
        }
    }
    unlock_sessions();
    return found;
}

static int update_checkpoint(session_t *session, uint32_t id, uint32_t index, uint64_t offset) {
    int valid = 0;
    lock_sessions();
    if (session && session->active && session->id == id && index < session->files.count && offset <= session->files.items[index].size) {
        if (offset == session->files.items[index].size) session->completed[index] = 1;
        session->last_activity = monotonic_seconds();
        valid = 1;
    }
    unlock_sessions();
    return valid ? 0 : -1;
}

static int validate_resume(session_t *session, uint32_t id, uint32_t index, uint64_t offset) {
    int valid = 0;
    lock_sessions();
    if (session && session->active && session->id == id && index < session->files.count &&
        offset <= session->files.items[index].size && !session->completed[index]) {
        session->last_activity = monotonic_seconds();
        valid = 1;
    }
    unlock_sessions();
    return valid ? 0 : -1;
}

static void finish_session(session_t *session) {
    lock_sessions();
    if (session) { session->active = 0; session->last_activity = monotonic_seconds(); }
    unlock_sessions();
}

static double random_unit(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    *state = x ? x : UINT64_C(0x9e3779b97f4a7c15);
    return (double)(*state >> 11) / 9007199254740992.0;
}

static int cmp_file(const void *a, const void *b) {
    const file_record *fa = (const file_record *)a;
    const file_record *fb = (const file_record *)b;
    return strcmp(fa->name, fb->name);
}

static void free_files(file_list *list) {
    size_t i;
    for (i = 0; i < list->count; ++i) free(list->items[i].name);
    free(list->items);
    memset(list, 0, sizeof(*list));
}

static int add_file(file_list *list, const char *name, uint64_t size) {
    file_record *next;
    char *copy;
    size_t n = strlen(name);
    if (n == 0 || n >= FNAME_LEN) return -1;
    copy = (char *)malloc(n + 1);
    if (!copy) return -1;
    memcpy(copy, name, n + 1);
    next = (file_record *)realloc(list->items, (list->count + 1) * sizeof(*next));
    if (!next) { free(copy); return -1; }
    list->items = next;
    list->items[list->count].name = copy;
    list->items[list->count].size = size;
    list->count++;
    return 0;
}

static char *trim_space(char *text) {
    char *end;
    while (*text && isspace((unsigned char)*text)) text++;
    end = text + strlen(text);
    while (end > text && isspace((unsigned char)end[-1])) *--end = '\0';
    return text;
}

static int add_topic(const char *name, const char *path) {
    size_t i;
    topic_t *next;
    char *name_copy, *path_copy;
    for (i = 0; i < topic_count; ++i) if (!strcmp(topics[i].name, name)) return -1;
    name_copy = (char *)malloc(strlen(name) + 1);
    path_copy = (char *)malloc(strlen(path) + 1);
    if (!name_copy || !path_copy) { free(name_copy); free(path_copy); return -1; }
    strcpy(name_copy, name); strcpy(path_copy, path);
    next = (topic_t *)realloc(topics, (topic_count + 1) * sizeof(*topics));
    if (!next) { free(name_copy); free(path_copy); return -1; }
    topics = next;
    topics[topic_count].name = name_copy;
    topics[topic_count].path = path_copy;
    topic_count++;
    return 0;
}

static int is_directory(const char *path) {
#ifdef _WIN32
    DWORD attributes = GetFileAttributesA(path);
    return attributes != INVALID_FILE_ATTRIBUTES && (attributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
#else
    struct stat st;
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
#endif
}

/* Optional topics.conf lines use: topic=directory (relative to the supplied root or absolute). */
static int load_topics_config(const char *root) {
    char config_path[PATH_CAP];
    char line[PATH_CAP + TOPIC_LEN + 8];
    FILE *config;
    if (join_path(config_path, sizeof(config_path), root, "topics.conf") != 0) return -1;
    config = fopen(config_path, "r");
    if (!config) return errno == ENOENT ? 1 : -1;
    while (fgets(line, sizeof(line), config)) {
        char *entry = trim_space(line), *equals, *name, *directory;
        char resolved[PATH_CAP];
        size_t len = strlen(entry);
        if (len == sizeof(line) - 1 && entry[len - 1] != '\n' && !feof(config)) { fclose(config); return -1; }
        if (!*entry || *entry == '#') continue;
        equals = strchr(entry, '=');
        if (!equals) { fclose(config); return -1; }
        *equals = '\0'; name = trim_space(entry); directory = trim_space(equals + 1);
        if (!valid_component(name) || strlen(name) >= TOPIC_LEN || !*directory) { fclose(config); return -1; }
#ifdef _WIN32
        if (directory[0] == '/' || directory[0] == '\\' || (isalpha((unsigned char)directory[0]) && directory[1] == ':')) {
#else
        if (directory[0] == '/') {
#endif
            if (strlen(directory) >= sizeof(resolved)) { fclose(config); return -1; }
            strcpy(resolved, directory);
        } else if (join_path(resolved, sizeof(resolved), root, directory) != 0) { fclose(config); return -1; }
        if (!is_directory(resolved) || add_topic(name, resolved) != 0) { fclose(config); return -1; }
    }
    if (ferror(config)) { fclose(config); return -1; }
    fclose(config);
    return 0;
}

static int scan_files(const char *dir_path, file_list *list) {
    memset(list, 0, sizeof(*list));
#ifdef _WIN32
    char pattern[PATH_CAP];
    WIN32_FIND_DATAA data;
    HANDLE find;
    if (join_path(pattern, sizeof(pattern), dir_path, "*") != 0) return -1;
    find = FindFirstFileA(pattern, &data);
    if (find == INVALID_HANDLE_VALUE) return GetLastError() == ERROR_FILE_NOT_FOUND ? 0 : -1;
    do {
        if (!(data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
            uint64_t size = ((uint64_t)data.nFileSizeHigh << 32) | data.nFileSizeLow;
            if (add_file(list, data.cFileName, size) != 0) { FindClose(find); free_files(list); return -1; }
        }
    } while (FindNextFileA(find, &data));
    FindClose(find);
#else
    DIR *dir = opendir(dir_path);
    struct dirent *entry;
    if (!dir) return -1;
    while ((entry = readdir(dir)) != NULL) {
        char path[PATH_CAP];
        struct stat st;
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
        if (join_path(path, sizeof(path), dir_path, entry->d_name) != 0 || stat(path, &st) != 0) continue;
        if (S_ISREG(st.st_mode) && st.st_size >= 0 && add_file(list, entry->d_name, (uint64_t)st.st_size) != 0) {
            closedir(dir); free_files(list); return -1;
        }
    }
    closedir(dir);
#endif
    if (list->count > 1) qsort(list->items, list->count, sizeof(*list->items), cmp_file);
    return 0;
}

static int load_topics(const char *root) {
    int config_result = load_topics_config(root);
    if (config_result == 0) return 0;
    if (config_result < 0) return -1;
#ifdef _WIN32
    char pattern[PATH_CAP];
    WIN32_FIND_DATAA data;
    HANDLE find;
    if (join_path(pattern, sizeof(pattern), root, "*") != 0) return -1;
    find = FindFirstFileA(pattern, &data);
    if (find == INVALID_HANDLE_VALUE) return -1;
    do {
        if ((data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) && strcmp(data.cFileName, ".") && strcmp(data.cFileName, "..")) {
            char path[PATH_CAP];
            if (strlen(data.cFileName) >= TOPIC_LEN || join_path(path, sizeof(path), root, data.cFileName) != 0) continue;
            if (add_topic(data.cFileName, path) != 0) { FindClose(find); return -1; }
        }
    } while (FindNextFileA(find, &data));
    FindClose(find);
#else
    DIR *dir = opendir(root);
    struct dirent *entry;
    if (!dir) return -1;
    while ((entry = readdir(dir)) != NULL) {
        char path[PATH_CAP];
        struct stat st;
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
        if (join_path(path, sizeof(path), root, entry->d_name) != 0 || stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        if (strlen(entry->d_name) >= TOPIC_LEN) { fprintf(stderr, "Skipping topic name too long: %s\n", entry->d_name); continue; }
        if (add_topic(entry->d_name, path) != 0) { closedir(dir); return -1; }
    }
    closedir(dir);
#endif
    return 0;
}

static const topic_t *find_topic(const char *name, size_t count, const topic_t *table) {
    size_t i;
    for (i = 0; i < count; ++i) if (!strcmp(name, table[i].name)) return &table[i];
    return NULL;
}

static int send_error(socket_t fd, uint64_t txid, uint16_t code, const char *text) {
    unsigned char payload[2 + 256];
    size_t n = strlen(text);
    if (n > 255) n = 255;
    put_u16(payload, code);
    memcpy(payload + 2, text, n);
    return send_message(fd, "ERR", txid, payload, (uint32_t)(n + 2));
}

/* ACK payload: kind, topic length+bytes, filename length+bytes, offset, chunk length. */
static int ack_matches(const message_t *m, uint64_t txid, uint32_t session_id, uint8_t kind,
                       const char *topic, const char *filename,
                       uint64_t offset, uint32_t chunk_length) {
    size_t topic_len = strlen(topic), file_len = strlen(filename), pos = 4;
    const unsigned char *p = m->payload;
    if (strcmp(m->type, "ACK") || m->transaction_id != txid || !p || m->payload_length < 4 + 1 + 2 + 2 + 8 + 4 || get_u32(p) != session_id) return 0;
    if (p[pos++] != kind) return 0;
    if (get_u16(p + pos) != topic_len) return 0;
    pos += 2;
    if (m->payload_length < pos + topic_len + 2 + 8 + 4) return 0;
    if (memcmp(p + pos, topic, topic_len)) return 0;
    pos += topic_len;
    if (get_u16(p + pos) != file_len) return 0;
    pos += 2;
    if (m->payload_length < pos + file_len + 12) return 0;
    if (memcmp(p + pos, filename, file_len)) return 0;
    pos += file_len;
    return get_u64(p + pos) == offset && get_u32(p + pos + 8) == chunk_length &&
           m->payload_length == pos + 12;
}

static int wait_for_ack(socket_t fd, uint64_t txid, uint32_t session_id, uint8_t kind,
                        const char *topic, const char *filename,
                        uint64_t offset, uint32_t chunk_length) {
    message_t ack;
    int ok;
    if (recv_message(fd, &ack) != 0) return -1;
    ok = ack_matches(&ack, txid, session_id, kind, topic, filename, offset, chunk_length);
    free_message(&ack);
    return ok ? 0 : -1;
}

static int send_session_list(socket_t fd, uint64_t txid, uint32_t session_id,
                             const char *topic, const file_list *files) {
    size_t topic_len = strlen(topic), total = 4 + 2 + topic_len + 4, i, pos;
    unsigned char *payload;
    if (topic_len > UINT16_MAX || files->count > UINT32_MAX) return -1;
    for (i = 0; i < files->count; ++i) {
        size_t n = strlen(files->items[i].name);
        if (n > UINT16_MAX || total > MAX_MESSAGE_PAYLOAD - (2 + n + 8)) return -1;
        total += 2 + n + 8;
    }
    payload = (unsigned char *)malloc(total);
    if (!payload) return -1;
    put_u32(payload, session_id); pos = 4;
    put_u16(payload + pos, (uint16_t)topic_len); pos += 2;
    memcpy(payload + pos, topic, topic_len); pos += topic_len;
    put_u32(payload + pos, (uint32_t)files->count); pos += 4;
    for (i = 0; i < files->count; ++i) {
        size_t n = strlen(files->items[i].name);
        put_u16(payload + pos, (uint16_t)n); pos += 2;
        memcpy(payload + pos, files->items[i].name, n); pos += n;
        put_u64(payload + pos, files->items[i].size); pos += 8;
    }
    i = send_message(fd, "SESSION", txid, payload, (uint32_t)total) == 0 ? 0 : 1;
    free(payload);
    return i == 0 ? 0 : -1;
}

static int send_file_header(socket_t fd, uint64_t txid, uint32_t session_id,
                            uint32_t file_index, const char *topic,
                            const file_record *file, uint64_t offset) {
    size_t topic_len = strlen(topic), file_len = strlen(file->name), total = 4 + 4 + 2 + topic_len + 2 + file_len + 8 + 8, pos = 0;
    unsigned char payload[4 + 4 + 2 + TOPIC_LEN + 2 + FNAME_LEN + 8 + 8];
    put_u32(payload + pos, session_id); pos += 4;
    put_u32(payload + pos, file_index); pos += 4;
    put_u16(payload + pos, (uint16_t)topic_len); pos += 2;
    memcpy(payload + pos, topic, topic_len); pos += topic_len;
    put_u16(payload + pos, (uint16_t)file_len); pos += 2;
    memcpy(payload + pos, file->name, file_len); pos += file_len;
    put_u64(payload + pos, file->size);
    pos += 8;
    put_u64(payload + pos, offset);
    return send_message(fd, "FILE_HDR", txid, payload, (uint32_t)total);
}

static int send_data_message(socket_t fd, uint64_t txid, uint32_t session_id,
                             uint32_t file_index, const char *topic,
                             const char *filename, uint64_t offset,
                             const unsigned char *data, uint32_t data_len) {
    size_t topic_len = strlen(topic), file_len = strlen(filename), header_len = 4 + 4 + 2 + topic_len + 2 + file_len + 8 + 4;
    unsigned char *payload = (unsigned char *)malloc(header_len + data_len);
    size_t pos = 0;
    int result;
    if (!payload) return -1;
    put_u32(payload + pos, session_id); pos += 4;
    put_u32(payload + pos, file_index); pos += 4;
    put_u16(payload + pos, (uint16_t)topic_len); pos += 2;
    memcpy(payload + pos, topic, topic_len); pos += topic_len;
    put_u16(payload + pos, (uint16_t)file_len); pos += 2;
    memcpy(payload + pos, filename, file_len); pos += file_len;
    put_u64(payload + pos, offset); pos += 8;
    put_u32(payload + pos, data_len); pos += 4;
    memcpy(payload + pos, data, data_len);
    result = send_message(fd, "DATA", txid, payload, (uint32_t)(header_len + data_len));
    free(payload);
    return result;
}

static int receive_checkpoint(socket_t fd, session_t *session, uint32_t session_id,
                              uint32_t file_index, uint64_t txid,
                              uint64_t expected_offset) {
    message_t message;
    int valid;
    (void)txid;
    if (recv_message(fd, &message) != 0) return -1;
    valid = !strcmp(message.type, "CHECKPOINT") && message.transaction_id == txid &&
            message.payload_length == 16 && get_u32(message.payload) == session_id &&
            get_u32(message.payload + 4) == file_index && get_u64(message.payload + 8) == expected_offset &&
            update_checkpoint(session, session_id, file_index, expected_offset) == 0;
    free_message(&message);
    if (!valid) return -1;
    return 0;
}


static int parse_topic_string(const unsigned char *payload, uint32_t length,
                              size_t *position, char *topic, size_t capacity) {
    uint16_t n;
    if (*position + 2 > length) return -1;
    n = get_u16(payload + *position); *position += 2;
    if (!n || n >= capacity || *position + n > length || memchr(payload + *position, '\0', n) ||
        memchr(payload + *position, '/', n) || memchr(payload + *position, '\\', n)) return -1;
    memcpy(topic, payload + *position, n); topic[n] = '\0'; *position += n;
    return 0;
}

static int seek_file(FILE *file, uint64_t offset) {
#ifdef _WIN32
    return _fseeki64(file, (__int64)offset, SEEK_SET);
#else
    return fseeko(file, (off_t)offset, SEEK_SET);
#endif
}

static int stream_session(socket_t fd, session_t *session, uint32_t start_index,
                          uint64_t start_offset, uint64_t *next_tx,
                          double probability, uint64_t *rng, uint64_t *data_bytes_sent) {
    uint64_t bytes_since_check = 0;
    uint32_t i;
    const topic_t *topic = find_topic(session->topic, topic_count, topics);
    if (!topic) return -1;
    for (i = start_index; i < session->files.count; ++i) {
        char path[PATH_CAP];
        FILE *in;
        uint64_t offset = i == start_index ? start_offset : 0;
        unsigned char buffer[IO_BUFFER_SIZE];
        uint64_t header_tx = (*next_tx)++;
        if (join_path(path, sizeof(path), topic->path, session->files.items[i].name) != 0 || !(in = fopen(path, "rb"))) {
            (void)send_error(fd, *next_tx, 5, "cannot open topic file"); return -1;
        }
        if (seek_file(in, offset) != 0 ||
            send_file_header(fd, header_tx, session->id, i, session->topic, &session->files.items[i], offset) != 0 ||
            wait_for_ack(fd, header_tx, session->id, 1, session->topic, session->files.items[i].name, offset, 0) != 0) {
            fclose(in); return -1;
        }
        while (offset < session->files.items[i].size) {
            size_t want = session->files.items[i].size - offset > sizeof(buffer) ? sizeof(buffer) : (size_t)(session->files.items[i].size - offset);
            size_t got = fread(buffer, 1, want, in);
            uint64_t data_tx = (*next_tx)++;
            uint64_t confirmed_offset;
            if (got != want || send_data_message(fd, data_tx, session->id, i, session->topic,
                    session->files.items[i].name, offset, buffer, (uint32_t)got) != 0) { fclose(in); return -1; }
            confirmed_offset = offset + got;
            if (receive_checkpoint(fd, session, session->id, i, data_tx, confirmed_offset) != 0) { fclose(in); return -1; }
            *data_bytes_sent += got;
            offset = confirmed_offset;
            bytes_since_check += got;
            if (bytes_since_check >= IO_BUFFER_SIZE) {
                bytes_since_check %= IO_BUFFER_SIZE;
                if (random_unit(rng) < probability) {
                    uint64_t event = atomic_fetch_add(&failures_injected, 1) + 1;
                    fprintf(stderr, "FAULT_INJECTED case=2 event=%llu probability=%.6f topic=%s file=%s file_offset=%llu checkpoint_confirmed=1\n",
                            (unsigned long long)event, probability, session->topic, session->files.items[i].name,
                            (unsigned long long)offset);
                    fclose(in); return -2;
                }
            }
        }
        fclose(in);
    }
    return 0;
}

static int serve_client(socket_t fd, const topic_t *table, size_t count,
                        double probability, uint64_t *rng) {
    message_t request;
    char topic_name[TOPIC_LEN];
    file_list files;
    session_t *session = NULL;
    uint64_t request_tx, next_tx, resume_offset = 0;
    uint32_t session_id, resume_index = 0;
    int result = -1;
    uint64_t data_bytes_sent = 0;
    (void)table; (void)count;
    memset(&files, 0, sizeof(files));
    if (recv_message(fd, &request) != 0) goto done;
    request_tx = request.transaction_id;
    if (!strcmp(request.type, "GET")) {
        const topic_t *topic;
        if (request.payload_length == 0 || request.payload_length >= sizeof(topic_name) ||
            memchr(request.payload, '\0', request.payload_length) || memchr(request.payload, '/', request.payload_length) ||
            memchr(request.payload, '\\', request.payload_length)) {
            (void)send_error(fd, request_tx, 1, "invalid GET request"); free_message(&request); goto done;
        }
        memcpy(topic_name, request.payload, request.payload_length); topic_name[request.payload_length] = '\0';
        free_message(&request);
        topic = find_topic(topic_name, topic_count, topics);
        if (!topic) { (void)send_error(fd, request_tx, 2, "unknown topic"); goto done; }
        if (scan_files(topic->path, &files) != 0 || files.count > UINT32_MAX) {
            (void)send_error(fd, request_tx, 3, "cannot read topic directory"); free_files(&files); goto done;
        }
        session = create_session(topic_name, &files);
        if (!session) { (void)send_error(fd, request_tx, 8, "session table full"); free_files(&files); goto done; }
        session_id = session->id;
        if (send_session_list(fd, request_tx, session_id, topic_name, &session->files) != 0) goto done;
        next_tx = request_tx + 1;
        if (recv_message(fd, &request) != 0) goto done;
        if (strcmp(request.type, "START") || request.transaction_id != next_tx || request.payload_length != 4 + strlen(topic_name) ||
            get_u32(request.payload) != session_id || memcmp(request.payload + 4, topic_name, strlen(topic_name))) {
            (void)send_error(fd, next_tx, 4, "invalid START request"); free_message(&request); goto done;
        }
        free_message(&request);
        next_tx++;
    } else if (!strcmp(request.type, "RESUME")) {
        size_t pos = 4;
        if (request.payload_length < 4 + 2 + 4 + 8) { (void)send_error(fd, request_tx, 1, "invalid RESUME request"); free_message(&request); goto done; }
        session_id = get_u32(request.payload);
        if (parse_topic_string(request.payload, request.payload_length, &pos, topic_name, sizeof(topic_name)) != 0 ||
            pos + 12 != request.payload_length) { (void)send_error(fd, request_tx, 1, "invalid RESUME request"); free_message(&request); goto done; }
        resume_index = get_u32(request.payload + pos); pos += 4;
        resume_offset = get_u64(request.payload + pos);
        free_message(&request);
        session = find_session(session_id, topic_name);
        if (!session) { (void)send_error(fd, request_tx, 6, "INVALID_SESSION"); goto done; }
        if (validate_resume(session, session_id, resume_index, resume_offset) != 0) {
            (void)send_error(fd, request_tx, 7, "INVALID_OFFSET"); goto done;
        }
        {
            unsigned char response[4]; put_u32(response, session_id);
            if (send_message(fd, "RESUME_OK", request_tx, response, sizeof(response)) != 0) goto done;
        }
        next_tx = request_tx + 1;
    } else {
        (void)send_error(fd, request_tx, 1, "expected GET or RESUME"); free_message(&request); goto done;
    }
    result = stream_session(fd, session, resume_index, resume_offset, &next_tx, probability, rng, &data_bytes_sent);
    if (result == 0) {
        unsigned char done_payload[4];
        put_u32(done_payload, session->id);
        if (send_message(fd, "DONE", next_tx, done_payload, sizeof(done_payload)) != 0) goto done;
        fprintf(stderr, "SESSION_COMPLETED id=%u topic=%s\n", session->id, session->topic);
        finish_session(session);
        result = 0;
    }
done:
    if (session) fprintf(stderr, "TRANSFER_BYTES case=2 session=%u topic=%s data_bytes=%llu\n", session->id,
                         session->topic, (unsigned long long)data_bytes_sent);
    free_files(&files);
    CLOSESOCKET(fd);
    return result;
}

#ifdef _WIN32
static DWORD WINAPI worker_main(LPVOID value) {
    worker_arg *arg = (worker_arg *)value;
    socket_t fd = arg->fd; const topic_t *table = arg->topics; size_t count = arg->topic_count;
    double probability = arg->probability; uint64_t seed = arg->seed;
    free(arg); (void)serve_client(fd, table, count, probability, &seed); return 0;
}
#else
static void *worker_main(void *value) {
    worker_arg *arg = (worker_arg *)value;
    socket_t fd = arg->fd; const topic_t *table = arg->topics; size_t count = arg->topic_count;
    double probability = arg->probability; uint64_t seed = arg->seed;
    free(arg); (void)serve_client(fd, table, count, probability, &seed); return NULL;
}
#endif

static int parse_port(const char *s, unsigned short *port) {
    char *end; long value;
    errno = 0; value = strtol(s, &end, 10);
    if (errno || !*s || *end || value < 1 || value > 65535) return -1;
    *port = (unsigned short)value; return 0;
}

int main(int argc, char **argv) {
    unsigned short port;
    char *prob_end;
    double probability;
    socket_t listener;
    struct sockaddr_in address;
    int opt = 1;
    if (argc != 4 || parse_port(argv[1], &port) != 0) {
        fprintf(stderr, "Usage: %s <port> <topics_root_dir> <failure_probability>\n", argv[0]); return 2;
    }
    errno = 0; probability = strtod(argv[3], &prob_end);
    if (errno || prob_end == argv[3] || *prob_end || !isfinite(probability) || probability < 0.0 || probability > 1.0) {
        fprintf(stderr, "Invalid failure_probability '%s': expected a number in the inclusive range 0.0 to 1.0\n", argv[3]); return 2;
    }
    if (socket_runtime_init() != 0) { report_socket_error("socket initialization"); return 1; }
#ifdef _WIN32
    InitializeCriticalSection(&session_mutex);
#endif
    if (load_topics(argv[2]) != 0) { fprintf(stderr, "Cannot scan topics root: %s\n", argv[2]); socket_runtime_cleanup(); return 1; }
    fprintf(stderr, "CONFIG case=2 failure_probability=%.6f check_interval_bytes=%u actual_injected_failures=0\n", probability, IO_BUFFER_SIZE);
    fprintf(stderr, "Loaded %lu topics; multi-threaded server listening on port %u\n", (unsigned long)topic_count, port);
    listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == (socket_t)-1) { report_socket_error("socket"); socket_runtime_cleanup(); return 1; }
#ifdef _WIN32
    (void)setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt, sizeof(opt));
#else
    (void)setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif
    memset(&address, 0, sizeof(address)); address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_ANY); address.sin_port = htons(port);
    if (bind(listener, (struct sockaddr *)&address, sizeof(address)) == SOCKET_ERR || listen(listener, SOMAXCONN) == SOCKET_ERR) {
        report_socket_error("bind/listen"); CLOSESOCKET(listener); socket_runtime_cleanup(); return 1;
    }
    for (;;) {
        struct sockaddr_in peer;
#ifdef _WIN32
        int peer_len = sizeof(peer);
#else
        socklen_t peer_len = sizeof(peer);
#endif
        socket_t fd = accept(listener, (struct sockaddr *)&peer, &peer_len);
        if (fd == (socket_t)-1) { report_socket_error("accept"); continue; }
        {
            worker_arg *arg = (worker_arg *)malloc(sizeof(*arg));
            worker_t thread;
            if (!arg) { CLOSESOCKET(fd); continue; }
            arg->fd = fd; arg->topics = topics; arg->topic_count = topic_count; arg->probability = probability;
            arg->seed = (uint64_t)time(NULL) ^ ((uint64_t)(uintptr_t)arg << 17) ^ (uint64_t)(uintptr_t)fd ^
                        (atomic_fetch_add(&seed_counter, 1) * UINT64_C(0x9e3779b97f4a7c15));
            if (arg->seed == 0) arg->seed = UINT64_C(0x123456789abcdef);
#ifdef _WIN32
            thread = CreateThread(NULL, 0, worker_main, arg, 0, NULL);
            if (!thread) { free(arg); CLOSESOCKET(fd); continue; }
            CloseHandle(thread);
#else
            if (pthread_create(&thread, NULL, worker_main, arg) != 0) { free(arg); CLOSESOCKET(fd); continue; }
            pthread_detach(thread);
#endif
        }
    }
}
