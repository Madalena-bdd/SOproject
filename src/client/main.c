/**
 * A program implementing a key-value store (KVS).
 * This file contains the main function of that program.
 * @file main.c
 * @authors:
 *  Madalena Bordadágua - 110382
 *  Madalena Martins - 110698
 */

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"


typedef struct ClientData{
  char req_pipe_path[MAX_STRING_SIZE];
  char resp_pipe_path[MAX_STRING_SIZE];
  char notif_pipe_path[MAX_STRING_SIZE];
  int req_fifo_fd;
  int resp_fifo_fd;
  int notif_fifo_fd;
  int client_subs;
  pthread_t notif_thread;
  _Atomic volatile sig_atomic_t terminate;
} ClientData;

/* ines
//initialize client data with the given client id
void initialize_client_data(char* client_id);

//cancel and join thread, unlink FIFOs and close file descriptors
void cleanup();

//signal handler for SIGINT and SIGTERM
void signal_handler();

//set uo signal handling for termination signals
void setup_signal_handling();

//checks for the SIGNIT and SIGTERM signals
void check_terminate_signal();

//create the required FIFOs for communication
int create_fifos();

//function assiggned to the notification thread
void* notifications_listner();


void handle_client_request(ClientData* client_data) {
  int req_fifo_fd = open(client_data->req_fifo_fd, O_RDONLY | O_NONBLOCK);
  int resp_fifo_fd = open(client_data->resp_fifo_fd, O_WRONLY); 
  int notif_fifo_fd = open(client_data->notif_fifo_fd, O_WRONLY);

  if (req_fifo_fd == -1 || resp_fifo_fd == -1 || notif_fifo_fd == -1) {
    perror("Error opening FIFOs");
    return;
  }

  send_mensage(resp_fifo_fd, OP_CODE_CONNECT, 0);

  char buffer[MAX_STRING_SIZE];
  while (!atomic_load(&client_data->terminate)) {
    ssize_t bytes_read = read(req_fifo_fd, buffer, MAX_STRING_SIZE);
    if (bytes_read > 0) {
      buffer[bytes_read] = '\0';
      char* token = strtok(buffer, "|");
      char* key;
      int op_code_int = atoi(token);
      enum OperationDofe op_code = (enum OperationDofe)op_code_int;
      switch (op_code) {
        case OP_CODE_SUBSCRIBE:
          key = strtok(NULL, "|");
          handle_client_subscriptions(resp_fifo_fd, notif_fifo_fd, key, OP_CODE_SUBSCRIBE);
          break;
        case OP_CODE_UNSUBSCRIBE: 
          key = strtok(NULL, "|");
          handle_client_subscriptions(resp_fifo_fd, -1, key, OP_CODE_UNSUBSCRIBE);
          break;
        case OP_CODE_DISCONNECT:
          handle_client_disconnect(resp_fifo_fd, req_fifo_fd, notif_fifo_fd);
          return;
        case OP_CODE_CONNECT:
          //this case is not read here since it is sent to the server pipe.
          break;
        default:
          fprintf(stderr, "Unknown operation code: %d\n", op_code);
          sen_message(resp_fifo_fd, op_code, 1);
          break;
      }
    }
  }
*/

int main(int argc, char* argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  char* server_pipe_path = argv[2]; // Caminho do FIFO do servidor

  int notif_pipe_fd; // Descriptor para o pipe de notificações

  if (kvs_connect(req_pipe_path, resp_pipe_path, server_pipe_path, notif_pipe_path, &notif_pipe_fd) != 0) { // Conectar ao servidor
    fprintf(stderr, "Erro ao conectar ao servidor\n");
    return 1;
  }

  printf("Conectado ao servidor com sucesso.\n");

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;
  // TO DO open pipes

  while (1) {
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
        // TODO: end notifications thread
        printf("Disconnected from server\n");
        return 0;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        } 
        if (kvs_subscribe(keys[0])) {
            fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_unsubscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
            printf("Waiting...\n");
            delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        // input should end in a disconnect, or it will loop here forever
        break;
    }
  }
}
