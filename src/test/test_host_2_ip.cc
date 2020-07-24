#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>

void check_host_name(int hostname) { //This function returns host name for local computer
   if (hostname == -1) {
      perror("gethostname");
      exit(1);
   }
}

void check_host_entry(struct hostent * hostentry) { //find host info from host name
   if (hostentry == NULL){
      perror("gethostbyname");
      exit(1);
   }
}

void IP_formatter(char *IPbuffer) { //convert IP string to dotted decimal format
   if (NULL == IPbuffer) {
      perror("inet_ntoa");
      exit(1);
   }
}

main(int argc, char** argv) {
   char host[256];
   if (argc > 1) {
      memcpy(host, argv[1], strlen(argv[1]));
   } else {
      int hostname;
      hostname = gethostname(host, sizeof(host)); //find the host name
      check_host_name(hostname);
   }

   printf("Current Host Name: %s\n", host);
   char* IP = NULL;

   struct hostent *host_entry;
   host_entry = gethostbyname(host); //find host information
   check_host_entry(host_entry);
   IP = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0])); //Convert into IP string
   printf("Host IP: %s\n", IP);
}