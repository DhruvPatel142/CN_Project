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
#define unlink _unlink
#define PATH_CAP 4096u
#else
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
    for (i = 1; i < n; ++i) {
        if (temp[i] == '/' || temp[i] == '\\') {
            char saved = temp[i]; temp[i] = '\0';
            if (*temp && make_directory(temp) != 0) { temp[i] = saved; return -1; }
            temp[i] = saved;
        }
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
                       char *dst, size_t capacity) {
    uint16_t n;
    if (*pos + 2 > length) return -1;
    n = get_u16(payload + *pos); *pos += 2;
    if (n == 0 || n >= capacity || *pos + n > length || memchr(payload + *pos, '\0', n)) return -1;
    memcpy(dst, payload + *pos, n); dst[n] = '\0'; *pos += n;
    return 0;
}

static int parse_file_list(const message_t *message, const char *topic, remote_list *list) {
    char parsed_topic[TOPIC_LEN];
    size_t pos = 0;
    uint32_t i;
    memset(list, 0, sizeof(*list));
    if (strcmp(message->type, "OK") || message->transaction_id != 1 || !message->payload) return -1;
    if (read_string(message->payload, message->payload_length, &pos, parsed_topic, sizeof(parsed_topic)) != 0 || strcmp(parsed_topic, topic) || pos + 4 > message->payload_length) return -1;
    list->count = get_u32(message->payload + pos); pos += 4;
    if (list->count > message->payload_length / 10u) return -1;
    if (list->count) {
        list->items = (remote_file *)calloc(list->count, sizeof(*list->items));
        if (!list->items) return -1;
    }
    for (i = 0; i < list->count; ++i) {
        char name[FNAME_LEN];
        if (read_string(message->payload, message->payload_length, &pos, name, sizeof(name)) != 0 || !valid_component(name) || pos + 8 > message->payload_length) { free_list(list); return -1; }
        list->items[i].name = (char *)malloc(strlen(name) + 1);
        if (!list->items[i].name) { free_list(list); return -1; }
        strcpy(list->items[i].name, name);
        list->items[i].size = get_u64(message->payload + pos); pos += 8;
    }
    if (pos != message->payload_length) { free_list(list); return -1; }
    return 0;
}

static int send_ack(socket_t fd, uint64_t txid, uint8_t kind, const char *topic,
                    const char *filename, uint64_t offset, uint32_t chunk_length) {
    size_t topic_len = strlen(topic), file_len = strlen(filename), pos = 0;
    size_t total = 1 + 2 + topic_len + 2 + file_len + 8 + 4;
    unsigned char payload[1 + 2 + TOPIC_LEN + 2 + FNAME_LEN + 8 + 4];
    payload[pos++] = kind;
    put_u16(payload + pos, (uint16_t)topic_len); pos += 2;
    memcpy(payload + pos, topic, topic_len); pos += topic_len;
    put_u16(payload + pos, (uint16_t)file_len); pos += 2;
    memcpy(payload + pos, filename, file_len); pos += file_len;
    put_u64(payload + pos, offset); pos += 8;
    put_u32(payload + pos, chunk_length);
    return send_message(fd, "ACK", txid, payload, (uint32_t)total);
}

static int server_error(const message_t *message) {
    if (!strcmp(message->type, "ERR")) {
        if (message->payload_length >= 2) fprintf(stderr, "Server error %u: %.*s\n", get_u16(message->payload), (int)(message->payload_length - 2), (const char *)(message->payload + 2));
        else fprintf(stderr, "Server returned an error.\n");
        return 1;
    }
    return 0;
}

static int receive_file_header(const message_t *m, const char *topic,
                               const remote_file *file, uint64_t expected_tx,
                               uint64_t *size) {
    char got_topic[TOPIC_LEN], got_name[FNAME_LEN];
    size_t pos = 0;
    if (strcmp(m->type, "FILE_HDR") || m->transaction_id != expected_tx || !m->payload) return -1;
    if (read_string(m->payload, m->payload_length, &pos, got_topic, sizeof(got_topic)) != 0 ||
        read_string(m->payload, m->payload_length, &pos, got_name, sizeof(got_name)) != 0 ||
        pos + 8 != m->payload_length || strcmp(got_topic, topic) || strcmp(got_name, file->name)) return -1;
    *size = get_u64(m->payload + pos);
    return *size == file->size ? 0 : -1;
}

static int receive_data(const message_t *m, const char *topic,
                        const remote_file *file, uint64_t expected_tx,
                        uint64_t expected_offset, FILE *out, uint32_t *chunk_length) {
    char got_topic[TOPIC_LEN], got_name[FNAME_LEN];
    size_t pos = 0;
    uint64_t offset;
    uint32_t n;
    if (strcmp(m->type, "DATA") || m->transaction_id != expected_tx || !m->payload) return -1;
    if (read_string(m->payload, m->payload_length, &pos, got_topic, sizeof(got_topic)) != 0 ||
        read_string(m->payload, m->payload_length, &pos, got_name, sizeof(got_name)) != 0 ||
        pos + 12 > m->payload_length || strcmp(got_topic, topic) || strcmp(got_name, file->name)) return -1;
    offset = get_u64(m->payload + pos); pos += 8;
    n = get_u32(m->payload + pos); pos += 4;
    if (offset != expected_offset || n == 0 || n > IO_BUFFER_SIZE || pos + n != m->payload_length ||
        (uint64_t)n > file->size - expected_offset || fwrite(m->payload + pos, 1, n, out) != n) return -1;
    *chunk_length = n;
    return 0;
}

static int receive_topic(socket_t fd, const char *topic, const char *output_dir,
                         uint64_t *total_bytes) {
    message_t message;
    remote_list files;
    char topic_dir[PATH_CAP];
    uint64_t server_tx = 3;
    uint32_t i;
    int result = -1;
    memset(&files, 0, sizeof(files));
    if (send_message(fd, "GET", 1, topic, (uint32_t)strlen(topic)) != 0) return -1;
    if (recv_message(fd, &message) != 0) return -1;
    if (server_error(&message)) {
        if (message.transaction_id != 1) fprintf(stderr, "Protocol error: mismatched error transaction ID\n");
        free_message(&message); return -2;
    }
    if (parse_file_list(&message, topic, &files) != 0) { free_message(&message); fprintf(stderr, "Protocol error: invalid OK file list\n"); return -1; }
    free_message(&message);
    if (make_directories(output_dir) != 0 || join_path(topic_dir, sizeof(topic_dir), output_dir, topic) != 0 || make_directories(topic_dir) != 0) {
        fprintf(stderr, "Cannot create output directory under: %s\n", output_dir); goto done;
    }
    if (send_message(fd, "START", 2, topic, (uint32_t)strlen(topic)) != 0) goto done;
    for (i = 0; i < files.count; ++i) {
        char path[PATH_CAP], part_path[PATH_CAP];
        uint64_t announced_size, offset = 0;
        FILE *out;
        uint64_t header_tx = server_tx++;
        if (recv_message(fd, &message) != 0) goto done;
        if (server_error(&message)) { free_message(&message); goto done; }
        if (receive_file_header(&message, topic, &files.items[i], header_tx, &announced_size) != 0) {
            free_message(&message); fprintf(stderr, "Protocol error: invalid FILE_HDR\n"); goto done;
        }
        free_message(&message);
        if (send_ack(fd, header_tx, 1, topic, files.items[i].name, 0, 0) != 0) goto done;
        if (join_path(path, sizeof(path), topic_dir, files.items[i].name) != 0 ||
            snprintf(part_path, sizeof(part_path), "%s.part", path) >= (int)sizeof(part_path)) goto done;
        out = fopen(part_path, "wb");
        if (!out) { perror(part_path); goto done; }
        while (offset < announced_size) {
            uint32_t chunk_length;
            uint64_t data_tx = server_tx++;
            if (recv_message(fd, &message) != 0) { fclose(out); unlink(part_path); goto done; }
            if (server_error(&message) || receive_data(&message, topic, &files.items[i], data_tx, offset, out, &chunk_length) != 0) {
                free_message(&message); fclose(out); unlink(part_path); fprintf(stderr, "Protocol error or transfer interrupted during DATA\n"); goto done;
            }
            free_message(&message);
            if (send_ack(fd, data_tx, 2, topic, files.items[i].name, offset, chunk_length) != 0) { fclose(out); unlink(part_path); goto done; }
            offset += chunk_length;
        }
        if (fclose(out) != 0) { unlink(part_path); goto done; }
#ifdef _WIN32
        (void)unlink(path);
#endif
        if (rename(part_path, path) != 0) { perror("rename"); unlink(part_path); goto done; }
        *total_bytes += announced_size;
    }
    if (recv_message(fd, &message) != 0) goto done;
    if (strcmp(message.type, "CLOSE") || message.transaction_id != server_tx) {
        free_message(&message); fprintf(stderr, "Protocol error: expected CLOSE\n"); goto done;
    }
    free_message(&message);
    result = 0;
done:
    free_list(&files);
    return result;
}

int main(int argc, char **argv) {
    char port[16];
    socket_t fd;
    uint64_t bytes = 0;
    int result;
    double start, end;
    if (argc != 5 || parse_port(argv[2], port, sizeof(port)) != 0 || !valid_component(argv[3]) || strlen(argv[3]) >= TOPIC_LEN) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir>\n", argv[0]); return 2;
    }
    if (socket_runtime_init() != 0) { report_socket_error("socket initialization"); return 1; }
    fd = connect_server(argv[1], port);
    if (fd == (socket_t)-1) { report_socket_error("connect"); socket_runtime_cleanup(); return 1; }
    start = monotonic_seconds();
    result = receive_topic(fd, argv[3], argv[4], &bytes);
    CLOSESOCKET(fd);
    end = monotonic_seconds();
    if (result == 0) {
        double seconds = end - start;
        fprintf(stderr, "Received %llu bytes in %.6f seconds (%.2f bytes/s)\n", (unsigned long long)bytes, seconds, seconds > 0 ? (double)bytes / seconds : 0.0);
    }
    socket_runtime_cleanup();
    return result == 0 ? 0 : 1;
}
