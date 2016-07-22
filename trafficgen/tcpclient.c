#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#define CHUNK_SIZE      500000

int main(int argc, char *argv[])
{
        int i;
	int sock;
	struct sockaddr_in server;
        char buffer[CHUNK_SIZE];
        long long size, num_chunks;
        struct timeval t1, t2;
        double elapsedTime;
        double speed;
        int port;
        int rest;
        struct sockaddr_in sin;
        socklen_t len;
        int src_port = 0;
        char src_addr[16];
        
        if (argc != 4) {
                printf("Usage: %s <server> <port> <size>\n", argv[0]);
                exit(0);
        }
        port = atoi(argv[2]);
        size = atoll(argv[3]);
        num_chunks = size/CHUNK_SIZE;
        rest = size % CHUNK_SIZE;
        
        //printf("Sending %lld bytes (%lld chunks, rest=%d)\n", size, num_chunks, rest);
        
	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock == -1) {
		perror("Could not create socket");
                return 1;
	}
        
	server.sin_addr.s_addr = inet_addr(argv[1]);
	server.sin_family = AF_INET;
	server.sin_port = htons(port);

	if (connect(sock, (struct sockaddr *)&server, sizeof(server)) < 0) {
		perror("connect failed. Error");
		return 1;
	}
        
        
        len = sizeof(sin);
        if (getsockname(sock, (struct sockaddr *)&sin, &len) == -1) {
                perror("getsockname");
        }
        
        src_port = ntohs(sin.sin_port);
        inet_ntop(AF_INET, &sin.sin_addr, src_addr, sizeof(src_addr));
        
        gettimeofday(&t1, NULL);

	for (i = 0; i < num_chunks; i++) {
		if (send(sock, buffer, CHUNK_SIZE, 0) < 0) {
			perror("Send failed");
			return 1;
		}
	}
        if (rest > 0) {
                if (send(sock, buffer, rest, 0) < 0) {
			perror("Send failed");
			return 1;
		}
        }
	gettimeofday(&t2, NULL);
        
        close(sock);
        
        elapsedTime = (t2.tv_sec - t1.tv_sec);
        elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000000.0;
        speed = (size*8.0)/elapsedTime;
        
        printf("%ld,%s,%d,%s,%d,%lld,%lf,%lf\n", t1.tv_sec, src_addr, src_port, argv[1], port, size, elapsedTime, speed/1000/1000);
        
	return 0;
}

