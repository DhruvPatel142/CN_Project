#ifndef PART1_COMMON_H
#define PART1_COMMON_H

#include <stddef.h>
#include <stdint.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
typedef SOCKET socket_t;
#define CLOSESOCKET closesocket
#define SOCKET_ERR SOCKET_ERROR
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
typedef int socket_t;
#define CLOSESOCKET close
#define SOCKET_ERR (-1)
#endif

#define MESSAGE_TYPE_LEN 16u
#define TOPIC_LEN 64u
#define FNAME_LEN 256u
#define IO_BUFFER_SIZE (64u * 1024u)
#define MAX_MESSAGE_PAYLOAD (16u * 1024u * 1024u)
#define WIRE_HEADER_SIZE 28u

typedef struct {
    char type[MESSAGE_TYPE_LEN + 1];
    uint64_t transaction_id;
    uint32_t payload_length;
    unsigned char *payload;
} message_t;

int socket_runtime_init(void);
void socket_runtime_cleanup(void);
int send_all(socket_t fd, const void *buffer, size_t length);
int recv_all(socket_t fd, void *buffer, size_t length);
int send_message(socket_t fd, const char *type, uint64_t transaction_id,
                 const void *payload, uint32_t payload_length);
int recv_message(socket_t fd, message_t *message);
void free_message(message_t *message);
int set_field(char *dst, size_t width, const char *src);
int valid_component(const char *name);
int make_directory(const char *path);
int join_path(char *dst, size_t cap, const char *a, const char *b);
uint16_t get_u16(const unsigned char *p);
uint32_t get_u32(const unsigned char *p);
uint64_t get_u64(const unsigned char *p);
void put_u16(unsigned char *p, uint16_t v);
void put_u32(unsigned char *p, uint32_t v);
void put_u64(unsigned char *p, uint64_t v);
void report_socket_error(const char *what);
double monotonic_seconds(void);

#endif
