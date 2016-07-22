#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>

#define CHUNK_SIZE      500000

int main(int argc, char *argv[])
{
	int socket_desc, client_sock, c, read_size;
	struct sockaddr_in server, client;
        char buffer[CHUNK_SIZE];
        int port;
        struct timeval t1, t2;
        double elapsedTime;
        double speed;
        long long size = 0;
        char src_addr[16];
        struct sockaddr_in *s;
        int src_port;
        
        if (argc != 3) {
                printf("Usage: %s <local_addr> <port>\n", argv[0]);
                exit(1);
        }
        port = atoi(argv[2]);
        
	socket_desc = socket(AF_INET, SOCK_STREAM, 0);
	if (socket_desc == -1) {
		perror("Could not create socket");
                return 1;
	}

	server.sin_family = AF_INET;
	server.sin_addr.s_addr = inet_addr(argv[1]);;
	server.sin_port = htons(port);

	if (bind(socket_desc, (struct sockaddr *)&server, sizeof(server)) < 0) {
		perror("bind failed. Error");
		return 1;
	}
        
	//Listen
	listen(socket_desc, 3);

	//Accept and incoming connection
	//printf("Waiting for incoming connections...\n");
	c = sizeof(struct sockaddr_in);

	//accept connection from an incoming client
	client_sock = accept(socket_desc, (struct sockaddr *)&client, (socklen_t *) & c);
	if (client_sock < 0) {
		perror("accept failed");
		return 1;
	}
	//printf("Connection accepted\n");

        s = (struct sockaddr_in *)&client;
        src_port = ntohs(s->sin_port);
        inet_ntop(AF_INET, &s->sin_addr, src_addr, sizeof(src_addr));
        
        gettimeofday(&t1, NULL);
        
	//Receive a message from client
	while ((read_size = recv(client_sock, buffer, CHUNK_SIZE, 0)) > 0) {
                size += read_size;
	}

        gettimeofday(&t2, NULL);

	//if (read_size == 0) {
	//	printf("Client disconnected\n");
	//	fflush(stdout);
	//} else if (read_size == -1) {
	//	perror("recv failed");
	//}

        elapsedTime = (t2.tv_sec - t1.tv_sec);
        elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000000.0;
        speed = (size*8.0)/elapsedTime;
        
        printf("%ld,%s,%d,%s,%d,%lld,%lf,%lf\n", t1.tv_sec, src_addr, src_port, argv[1], port, size, elapsedTime, speed/1000/1000);
        
	return 0;
}
