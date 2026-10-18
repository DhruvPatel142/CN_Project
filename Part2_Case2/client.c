#define _POSIX_C_SOURCE 200809L
#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <sys/stat.h>
#define unlink _unlink
#define PATH_CAP 4096u
#else
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#include <netdb.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#define PATH_CAP ((size_t)PATH_MAX)
#endif

typedef struct { char *name; uint64_t size; } remote_file;
typedef struct { remote_file *items; uint32_t count; } remote_list;

static int parse_port(const char *s, char *endbuf, size_t cap) {
    char *end; long n;
    errno = 0; n = strtol(s, &end, 10);
    if (errno || !*s || *end || n < 1 || n > 65535 || strlen(s) >= cap) return -1;
    strcpy(endbuf, s); return 0;
}

static int make_directories(const char *path) {
    char temp[PATH_CAP]; size_t i, n = strlen(path);
    if (n == 0 || n >= sizeof(temp)) return -1;
    memcpy(temp, path, n + 1);
    for (i = 1; i < n; ++i) if (temp[i] == '/' || temp[i] == '\\') {
        char saved = temp[i]; temp[i] = '\0';
        if (*temp && make_directory(temp) != 0) { temp[i] = saved; return -1; }
        temp[i] = saved;
    }
    return make_directory(temp);
}

static socket_t connect_server(const char *host, const char *port) {
    struct addrinfo hints, *results = NULL, *it;
    socket_t fd = (socket_t)-1;
    memset(&hints, 0, sizeof(hints)); hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &results) != 0) return (socket_t)-1;
    for (it = results; it; it = it->ai_next) {
        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd == (socket_t)-1) continue;
        if (connect(fd, it->ai_addr, (int)it->ai_addrlen) == 0) break;
        CLOSESOCKET(fd); fd = (socket_t)-1;
    }
    freeaddrinfo(results);
    return fd;
}

static void free_list(remote_list *list) {
    uint32_t i;
    for (i = 0; i < list->count; ++i) free(list->items[i].name);
    free(list->items); memset(list, 0, sizeof(*list));
}

static int read_string(const unsigned char *payload, uint32_t length, size_t *pos,
                       char *dst, size_t dst_size) {
    uint16_t n;
    if (*pos + 2 > length) return -1;
    n = get_u16(payload + *pos); *pos += 2;
    if (!n || n >= dst_size || *pos + n > length || memchr(payload + *pos, '\0', n)) return -1;
    memcpy(dst, payload + *pos, n); dst[n] = '\0'; *pos += n;
    return 0;
}

static int parse_session(const message_t *message, const char *topic,
                         uint32_t *session_id, remote_list *files) {
    char parsed_topic[TOPIC_LEN];
    size_t pos = 4;
    uint32_t i;
    memset(files, 0, sizeof(*files));
    if (strcmp(message->type, "SESSION") || message->transaction_id != 1 || !message->payload || message->payload_length < 10) return -1;
    *session_id = get_u32(message->payload);
    if (*session_id == 0 || read_string(message->payload, message->payload_length, &pos, parsed_topic, sizeof(parsed_topic)) != 0 ||
        strcmp(parsed_topic, topic) || pos + 4 > message->payload_length) return -1;
    files->count = get_u32(message->payload + pos); pos += 4;
    if (files->count > message->payload_length / 10u) return -1;
    if (files->count) {
        files->items = (remote_file *)calloc(files->count, sizeof(*files->items));
        if (!files->items) return -1;
    }
    for (i = 0; i < files->count; ++i) {
        char name[FNAME_LEN];
        if (read_string(message->payload, message->payload_length, &pos, name, sizeof(name)) != 0 || !valid_component(name) || pos + 8 > message->payload_length) { free_list(files); return -1; }
        files->items[i].name = (char *)malloc(strlen(name) + 1);
        if (!files->items[i].name) { free_list(files); return -1; }
        strcpy(files->items[i].name, name);
        files->items[i].size = get_u64(message->payload + pos); pos += 8;
    }
    if (pos != message->payload_length) { free_list(files); return -1; }
    return 0;
}

static int server_error(const message_t *message, uint16_t *code) {
    if (strcmp(message->type, "ERR")) return 0;
    *code = message->payload_length >= 2 ? get_u16(message->payload) : 0;
    if (message->payload_length > 2)
        fprintf(stderr, "Server error %u: %.*s\n", *code, (int)(message->payload_length - 2), (const char *)(message->payload + 2));
    else fprintf(stderr, "Server returned error %u\n", *code);
    return 1;
}

static int send_header_ack(socket_t fd, uint64_t txid, uint32_t session_id, const char *topic,
                           const char *filename, uint64_t offset) {
    size_t topic_len = strlen(topic), file_len = strlen(filename), pos = 0;
    unsigned char payload[4 + 1 + 2 + TOPIC_LEN + 2 + FNAME_LEN + 8 + 4];
    size_t len = 4 + 1 + 2 + topic_len + 2 + file_len + 8 + 4;
    put_u32(payload + pos, session_id); pos += 4;
    payload[pos++] = 1;
    put_u16(payload + pos, (uint16_t)topic_len); pos += 2;
    memcpy(payload + pos, topic, topic_len); pos += topic_len;
    put_u16(payload + pos, (uint16_t)file_len); pos += 2;
    memcpy(payload + pos, filename, file_len); pos += file_len;
    put_u64(payload + pos, offset); pos += 8;
    put_u32(payload + pos, 0);
    return send_message(fd, "ACK", txid, payload, (uint32_t)len);
}

static int parse_file_header(const message_t *m, uint32_t session_id, uint32_t index,
                             const char *topic, const remote_file *file,
                             uint64_t expected_tx, uint64_t expected_offset) {
    char got_topic[TOPIC_LEN], got_name[FNAME_LEN];
    size_t pos = 8;
    uint64_t size, offset;
    if (strcmp(m->type, "FILE_HDR") || m->transaction_id != expected_tx || !m->payload || m->payload_length < 28 ||
        get_u32(m->payload) != session_id || get_u32(m->payload + 4) != index) return -1;
    if (read_string(m->payload, m->payload_length, &pos, got_topic, sizeof(got_topic)) != 0 ||
        read_string(m->payload, m->payload_length, &pos, got_name, sizeof(got_name)) != 0 || pos + 16 != m->payload_length ||
        strcmp(got_topic, topic) || strcmp(got_name, file->name)) return -1;
    size = get_u64(m->payload + pos); offset = get_u64(m->payload + pos + 8);
    return size == file->size && offset == expected_offset ? 0 : -1;
}

static int seek_file(FILE *file, uint64_t offset) {
#ifdef _WIN32
    return _fseeki64(file, (__int64)offset, SEEK_SET);
#else
    return fseeko(file, (off_t)offset, SEEK_SET);
#endif
}

static int truncate_file(FILE *file, uint64_t size) {
    if (fflush(file) != 0) return -1;
#ifdef _WIN32
    return _chsize_s(_fileno(file), size) == 0 ? 0 : -1;
#else
    return ftruncate(fileno(file), (off_t)size);
#endif
}

static int output_file_has_size(const char *path, uint64_t expected) {
#ifdef _WIN32
    struct _stat64 st;
    return _stat64(path, &st) == 0 && st.st_size >= 0 && (uint64_t)st.st_size == expected;
#else
    struct stat st;
    return stat(path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size >= 0 && (uint64_t)st.st_size == expected;
#endif
}

static int commit_part_file(const char *part, const char *final) {
#ifdef _WIN32
    (void)unlink(final);
#endif
    return rename(part, final);
}

static int send_checkpoint(socket_t fd, uint64_t txid, uint32_t session_id,
                           uint32_t index, uint64_t offset) {
    unsigned char payload[16];
    put_u32(payload, session_id); put_u32(payload + 4, index); put_u64(payload + 8, offset);
    return send_message(fd, "CHECKPOINT", txid, payload, sizeof(payload));
}

static int receive_data(const message_t *m, uint32_t session_id, uint32_t index,
                        const char *topic, const remote_file *file, uint64_t expected_tx,
                        uint64_t expected_offset, FILE *out, uint32_t *chunk_length) {
    char got_topic[TOPIC_LEN], got_name[FNAME_LEN];
    size_t pos = 8;
    uint64_t offset;
    uint32_t n;
    if (strcmp(m->type, "DATA") || m->transaction_id != expected_tx || !m->payload || m->payload_length < 28 ||
        get_u32(m->payload) != session_id || get_u32(m->payload + 4) != index) return -1;
    if (read_string(m->payload, m->payload_length, &pos, got_topic, sizeof(got_topic)) != 0 ||
        read_string(m->payload, m->payload_length, &pos, got_name, sizeof(got_name)) != 0 ||
        pos + 12 > m->payload_length || strcmp(got_topic, topic) || strcmp(got_name, file->name)) return -1;
    offset = get_u64(m->payload + pos); pos += 8; n = get_u32(m->payload + pos); pos += 4;
    if (offset != expected_offset || n == 0 || n > IO_BUFFER_SIZE || pos + n != m->payload_length ||
        (uint64_t)n > file->size - expected_offset || fwrite(m->payload + pos, 1, n, out) != n || fflush(out) != 0) return -1;
    *chunk_length = n;
    return 0;
}

static int transfer_session(socket_t fd, const char *topic, const char *topic_dir,
                            remote_list *files, uint32_t session_id, uint64_t *offsets,
                            uint32_t start_index, uint64_t start_offset, uint64_t server_tx) {
    uint32_t i;
    for (i = start_index; i < files->count; ++i) {
        char final_path[PATH_CAP], part_path[PATH_CAP];
        uint64_t file_offset = i == start_index ? start_offset : offsets[i];
        uint64_t header_tx = server_tx++;
        FILE *out = NULL;
        message_t message;
        uint64_t size = files->items[i].size;
        if (join_path(final_path, sizeof(final_path), topic_dir, files->items[i].name) != 0 ||
            snprintf(part_path, sizeof(part_path), "%s.part", final_path) >= (int)sizeof(part_path)) return -2;
        if (recv_message(fd, &message) != 0) return -1;
        if (strcmp(message.type, "FILE_HDR") && !strcmp(message.type, "ERR")) {
            uint16_t code = 0; (void)server_error(&message, &code); free_message(&message); return -2;
        }
        if (parse_file_header(&message, session_id, i, topic, &files->items[i], header_tx, file_offset) != 0) {
            free_message(&message); fprintf(stderr, "Protocol error: invalid FILE_HDR for index %u\n", i); return -2;
        }
        free_message(&message);
        if (file_offset < size) {
            out = file_offset ? fopen(part_path, "r+b") : fopen(part_path, "wb+");
            if (!out || truncate_file(out, file_offset) != 0 || seek_file(out, file_offset) != 0) {
                if (out) fclose(out);
                fprintf(stderr, "Cannot open/seek partial file %s\n", part_path);
                return -2;
            }
        } else if (size > 0 && !output_file_has_size(final_path, size)) {
            fprintf(stderr, "Checkpoint says file is complete but local file is missing or wrong-sized: %s\n", final_path); return -2;
        }
        if (send_header_ack(fd, header_tx, session_id, topic, files->items[i].name, file_offset) != 0) { if (out) fclose(out); return -1; }
        while (file_offset < size) {
            uint32_t chunk_length;
            uint64_t data_tx = server_tx++;
            if (recv_message(fd, &message) != 0) { fclose(out); return -1; }
            if (strcmp(message.type, "DATA") && !strcmp(message.type, "ERR")) {
                uint16_t code = 0; (void)server_error(&message, &code); free_message(&message); fclose(out); return -2;
            }
            if (receive_data(&message, session_id, i, topic, &files->items[i], data_tx, file_offset, out, &chunk_length) != 0) {
                free_message(&message); fclose(out); fprintf(stderr, "Protocol error while receiving file %s\n", files->items[i].name); return -2;
            }
            free_message(&message);
            if (send_checkpoint(fd, data_tx, session_id, i, file_offset + chunk_length) != 0) { fclose(out); return -1; }
            file_offset += chunk_length;
            offsets[i] = file_offset;
        }
        if (out) {
            if (fclose(out) != 0 || commit_part_file(part_path, final_path) != 0) { fprintf(stderr, "Cannot commit %s\n", final_path); return -2; }
        } else if (size == 0 && !output_file_has_size(final_path, 0)) {
            out = fopen(part_path, "wb");
            if (!out || fclose(out) != 0 || commit_part_file(part_path, final_path) != 0) { if (out) unlink(part_path); return -2; }
        }
        offsets[i] = size;
    }
    {
        message_t done;
        if (recv_message(fd, &done) != 0) return -1;
        if (strcmp(done.type, "DONE") || done.transaction_id != server_tx || done.payload_length != 4 || get_u32(done.payload) != session_id) {
            free_message(&done); fprintf(stderr, "Protocol error: expected DONE\n"); return -2;
        }
        free_message(&done);
    }
    return 0;
}

static int begin_session(socket_t fd, const char *topic, const char *output_dir,
                         uint32_t *session_id, remote_list *files, uint64_t **offsets,
                         char *topic_dir, size_t topic_dir_cap) {
    message_t response;
    unsigned char start_payload[4 + TOPIC_LEN];
    if (send_message(fd, "GET", 1, topic, (uint32_t)strlen(topic)) != 0 || recv_message(fd, &response) != 0) return -1;
    if (!strcmp(response.type, "ERR")) {
        uint16_t code = 0; (void)server_error(&response, &code); free_message(&response); return code == 2 ? -2 : -1;
    }
    if (parse_session(&response, topic, session_id, files) != 0) { free_message(&response); fprintf(stderr, "Protocol error: invalid SESSION response\n"); return -2; }
    free_message(&response);
    if (join_path(topic_dir, topic_dir_cap, output_dir, topic) != 0 || make_directories(topic_dir) != 0) return -2;
    if (files->count) {
        *offsets = (uint64_t *)calloc(files->count, sizeof(**offsets));
        if (!*offsets) { *session_id = 0; return -2; }
    }
    put_u32(start_payload, *session_id);
    memcpy(start_payload + 4, topic, strlen(topic));
    if (send_message(fd, "START", 2, start_payload, (uint32_t)(4 + strlen(topic))) != 0) return -1;
    return 0;
}

static int resume_session(socket_t fd, const char *topic, uint32_t session_id,
                          uint32_t index, uint64_t offset, uint16_t *error_code) {
    size_t topic_len = strlen(topic), pos = 0;
    unsigned char payload[4 + 2 + TOPIC_LEN + 4 + 8];
    message_t response;
    put_u32(payload + pos, session_id); pos += 4;
    put_u16(payload + pos, (uint16_t)topic_len); pos += 2;
    memcpy(payload + pos, topic, topic_len); pos += topic_len;
    put_u32(payload + pos, index); pos += 4; put_u64(payload + pos, offset); pos += 8;
    if (send_message(fd, "RESUME", 1, payload, (uint32_t)pos) != 0 || recv_message(fd, &response) != 0) return -1;
    if (server_error(&response, error_code)) { free_message(&response); return -2; }
    if (strcmp(response.type, "RESUME_OK") || response.transaction_id != 1 || response.payload_length != 4 || get_u32(response.payload) != session_id) {
        free_message(&response); return -1;
    }
    free_message(&response);
    return 0;
}

static void clear_partial_files(const char *topic_dir, const remote_list *files) {
    uint32_t i;
    for (i = 0; i < files->count; ++i) {
        char path[PATH_CAP];
        if (snprintf(path, sizeof(path), "%s/%s.part", topic_dir, files->items[i].name) < (int)sizeof(path)) (void)unlink(path);
    }
}

int main(int argc, char **argv) {
    char port[16], topic_dir[PATH_CAP];
    remote_list files;
    uint32_t session_id = 0, retries = 0, start_index;
    uint64_t *offsets = NULL, start_offset;
    int result = -1, needs_new_session = 1;
    double start, end;
    if (argc != 5 || parse_port(argv[2], port, sizeof(port)) != 0 || !valid_component(argv[3]) || strlen(argv[3]) >= TOPIC_LEN) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir>\n", argv[0]); return 2;
    }
    memset(&files, 0, sizeof(files));
    if (socket_runtime_init() != 0) { report_socket_error("socket initialization"); return 1; }
    if (make_directories(argv[4]) != 0 || join_path(topic_dir, sizeof(topic_dir), argv[4], argv[3]) != 0 || make_directories(topic_dir) != 0) {
        fprintf(stderr, "Cannot create output directory under %s\n", argv[4]); socket_runtime_cleanup(); return 1;
    }
    start = monotonic_seconds();
    while (retries < 10000) {
        socket_t fd = connect_server(argv[1], port);
        if (fd == (socket_t)-1) { report_socket_error("connect"); ++retries; continue; }
        if (needs_new_session) {
            result = begin_session(fd, argv[3], argv[4], &session_id, &files, &offsets, topic_dir, sizeof(topic_dir));
            if (result == 0) {
                result = transfer_session(fd, argv[3], topic_dir, &files, session_id, offsets, 0, 0, 3);
            }
            CLOSESOCKET(fd);
            if (result == 0) break;
            if (result == -2) break;
            if (session_id != 0) needs_new_session = 0;
        } else {
            uint16_t error_code = 0;
            uint32_t i;
            for (i = 0; i < files.count && offsets[i] >= files.items[i].size; ++i) { }
            if (i < files.count) { start_index = i; start_offset = offsets[i]; }
            else if (files.count) { start_index = files.count - 1; start_offset = files.items[start_index].size; }
            else { CLOSESOCKET(fd); session_id = 0; needs_new_session = 1; ++retries; continue; }
            result = resume_session(fd, argv[3], session_id, start_index, start_offset, &error_code);
            if (result == -2 && (error_code == 6 || error_code == 7)) {
                CLOSESOCKET(fd);
                fprintf(stderr, "CASE2_INVALID_CHECKPOINT fallback=fresh_GET\n");
                clear_partial_files(topic_dir, &files);
                free(offsets); offsets = NULL; free_list(&files);
                session_id = 0; needs_new_session = 1; ++retries; continue;
            }
            if (result == 0) result = transfer_session(fd, argv[3], topic_dir, &files, session_id, offsets, start_index, start_offset, 2);
            CLOSESOCKET(fd);
            if (result == 0) break;
        }
        if (result == -2) break;
        ++retries;
        fprintf(stderr, "CASE2_RETRY attempt=%u action=resume_session=%u\n", retries, session_id);
    }
    end = monotonic_seconds();
    if (retries >= 10000 && result != 0) fprintf(stderr, "Transfer stopped after 10000 reconnect attempts\n");
    if (result == 0) {
        uint64_t useful_bytes = 0; uint32_t i;
        for (i = 0; i < files.count; ++i) useful_bytes += files.items[i].size;
        fprintf(stderr, "CASE2 completed session=%u retries=%u useful_bytes=%llu time_seconds=%.6f throughput_bytes_per_second=%.2f\n",
                session_id, retries, (unsigned long long)useful_bytes, end - start,
                end > start ? (double)useful_bytes / (end - start) : 0.0);
    }
    clear_partial_files(topic_dir, &files);
    free(offsets); free_list(&files); socket_runtime_cleanup();
    return result == 0 ? 0 : 1;
}
