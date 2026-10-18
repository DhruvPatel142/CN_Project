CC ?= cc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -Wpedantic
LDFLAGS ?=

ifeq ($(OS),Windows_NT)
THREAD_FLAGS =
SOCKET_LIBS = -lws2_32
else
THREAD_FLAGS = -pthread
SOCKET_LIBS =
endif

.PHONY: all clean
all: server client

server: server.c common.c common.h
	$(CC) $(CFLAGS) $(THREAD_FLAGS) server.c common.c $(LDFLAGS) $(SOCKET_LIBS) -o $@

client: client.c common.c common.h
	$(CC) $(CFLAGS) $(THREAD_FLAGS) client.c common.c $(LDFLAGS) $(SOCKET_LIBS) -o $@

clean:
	rm -f server client
