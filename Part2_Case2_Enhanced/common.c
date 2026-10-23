#define _POSIX_C_SOURCE 200809L
#include "common.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include <stdlib.h>

#ifdef _WIN32
#include <direct.h>
#else
#include <sys/stat.h>
#include <signal.h>
#endif

int socket_runtime_init(void) {
#ifdef _WIN32
    WSADATA data;
    return WSAStartup(MAKEWORD(2, 2), &data) == 0 ? 0 : -1;
#else
    signal(SIGPIPE, SIG_IGN);
    return 0;
#endif
}

void socket_runtime_cleanup(void) {
#ifdef _WIN32
    WSACleanup();
#endif
}

int send_all(socket_t fd, const void *buffer, size_t length) {
    /* A successful send can write only part of a frame, so keep sending the remainder. */
    const char *p = (const char *)buffer;
    size_t done = 0;
    while (done < length) {
        size_t left = length - done;
#ifdef _WIN32
        int chunk = left > INT_MAX ? INT_MAX : (int)left;
        int n = send(fd, p + done, chunk, 0);
#else
        ssize_t n = send(fd, p + done, left, 0);
#endif
#ifdef _WIN32
        if (n == SOCKET_ERROR && WSAGetLastError() == WSAEINTR) continue;
#else
        if (n < 0 && errno == EINTR) continue;
#endif
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return 0;
}

int recv_all(socket_t fd, void *buffer, size_t length) {
    /* TCP is a byte stream; one recv is not guaranteed to fill the requested block. */
    char *p = (char *)buffer;
    size_t done = 0;
    while (done < length) {
        size_t left = length - done;
#ifdef _WIN32
        int chunk = left > INT_MAX ? INT_MAX : (int)left;
        int n = recv(fd, p + done, chunk, 0);
#else
        ssize_t n = recv(fd, p + done, left, 0);
#endif
#ifdef _WIN32
        if (n == SOCKET_ERROR && WSAGetLastError() == WSAEINTR) continue;
#else
        if (n < 0 && errno == EINTR) continue;
#endif
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return 0;
}

static uint64_t host_to_net64(uint64_t x) {
    const uint16_t one = 1;
    if (*(const unsigned char *)&one == 1) {
        return ((uint64_t)htonl((uint32_t)(x & UINT64_C(0xffffffff))) << 32) |
               htonl((uint32_t)(x >> 32));
    }
    return x;
}

static uint64_t net_to_host64(uint64_t x) { return host_to_net64(x); }

uint16_t get_u16(const unsigned char *p) {
    uint16_t v;
    memcpy(&v, p, sizeof(v));
    return ntohs(v);
}
uint32_t get_u32(const unsigned char *p) {
    uint32_t v;
    memcpy(&v, p, sizeof(v));
    return ntohl(v);
}
uint64_t get_u64(const unsigned char *p) {
    uint64_t v;
    memcpy(&v, p, sizeof(v));
    return net_to_host64(v);
}
void put_u16(unsigned char *p, uint16_t v) {
    v = htons(v);
    memcpy(p, &v, sizeof(v));
}
void put_u32(unsigned char *p, uint32_t v) {
    v = htonl(v);
    memcpy(p, &v, sizeof(v));
}
void put_u64(unsigned char *p, uint64_t v) {
    v = host_to_net64(v);
    memcpy(p, &v, sizeof(v));
}

int send_message(socket_t fd, const char *type, uint64_t transaction_id,
                 const void *payload, uint32_t payload_length) {
    unsigned char header[WIRE_HEADER_SIZE] = {0};
    unsigned char *frame;
    size_t type_length = strlen(type);
    int result;
    if (type_length == 0 || type_length >= MESSAGE_TYPE_LEN || payload_length > MAX_MESSAGE_PAYLOAD ||
        (payload_length && !payload)) return -1;
    memcpy(header, type, type_length);
    put_u64(header + MESSAGE_TYPE_LEN, transaction_id);
    put_u32(header + MESSAGE_TYPE_LEN + 8u, payload_length);
    frame = (unsigned char *)malloc(sizeof(header) + payload_length);
    if (!frame) return -1;
    memcpy(frame, header, sizeof(header));
    if (payload_length) memcpy(frame + sizeof(header), payload, payload_length);
    result = send_all(fd, frame, sizeof(header) + payload_length);
    free(frame);
    return result;
}

int recv_message(socket_t fd, message_t *message) {
    unsigned char header[WIRE_HEADER_SIZE];
    uint32_t length;
    memset(message, 0, sizeof(*message));
    if (recv_all(fd, header, sizeof(header)) != 0) return -1;
    if (!memchr(header, '\0', MESSAGE_TYPE_LEN)) return -1;
    memcpy(message->type, header, MESSAGE_TYPE_LEN);
    message->type[MESSAGE_TYPE_LEN] = '\0';
    message->transaction_id = get_u64(header + MESSAGE_TYPE_LEN);
    length = get_u32(header + MESSAGE_TYPE_LEN + 8u);
    if (length > MAX_MESSAGE_PAYLOAD) return -1;
    message->payload_length = length;
    if (length) {
        message->payload = (unsigned char *)malloc(length);
        if (!message->payload) return -1;
        if (recv_all(fd, message->payload, length) != 0) {
            free(message->payload);
            memset(message, 0, sizeof(*message));
            return -1;
        }
    }
    return 0;
}

void free_message(message_t *message) {
    free(message->payload);
    memset(message, 0, sizeof(*message));
}

int set_field(char *dst, size_t width, const char *src) {
    size_t n = strlen(src);
    if (n >= width) return -1;
    memset(dst, 0, width);
    memcpy(dst, src, n);
    return 0;
}

int make_directory(const char *path) {
#ifdef _WIN32
    if (_mkdir(path) == 0 || errno == EEXIST) return 0;
#else
    if (mkdir(path, 0777) == 0 || errno == EEXIST) return 0;
#endif
    return -1;
}

int join_path(char *dst, size_t cap, const char *a, const char *b) {
    size_t na = strlen(a), nb = strlen(b);
    char sep = '/';
#ifdef _WIN32
    sep = '\\';
#endif
    int need_sep = na > 0 && a[na - 1] != '/' && a[na - 1] != '\\';
    if (na + (size_t)need_sep + nb + 1 > cap) return -1;
    memcpy(dst, a, na);
    if (need_sep) dst[na++] = sep;
    memcpy(dst + na, b, nb + 1);
    return 0;
}

int valid_component(const char *name) {
    if (!name || !*name || !strcmp(name, ".") || !strcmp(name, "..")) return 0;
    return strchr(name, '/') == NULL && strchr(name, '\\') == NULL;
}

void report_socket_error(const char *what) {
#ifdef _WIN32
    fprintf(stderr, "%s failed (socket error %d)\n", what, WSAGetLastError());
#else
    fprintf(stderr, "%s failed: %s\n", what, strerror(errno));
#endif
}

double monotonic_seconds(void) {
#ifdef _WIN32
    LARGE_INTEGER count, frequency;
    if (!QueryPerformanceFrequency(&frequency) || !QueryPerformanceCounter(&count)) return 0.0;
    return (double)count.QuadPart / (double)frequency.QuadPart;
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0.0;
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
#endif
}
