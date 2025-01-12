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

int main(int argc, char* argv[]) {

  if (argc < 3) { //testar com 4 argumentos!!!
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  // Inicialização dos caminhos bases para os fifos de pedidos
  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  // Torna o caminho único (adiciona o id) -> ex: "/tmp/req1234"
  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char)); 
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  char* server_pipe_path = argv[2]; // Caminho do FIFO do servidor


  //FIX MEEEE: "notif_pipe_fd" variável não utilizada!
  int notif_pipe_fd; // Descriptor para o pipe de notificações

  // Chama o kvs_connect para se conectar ao servidor
  if (kvs_connect(req_pipe_path, resp_pipe_path, server_pipe_path, notif_pipe_path, &notif_pipe_fd) != 0) { // Conectar ao servidor
    fprintf(stderr, "Erro ao conectar ao servidor\n");
    return 1;
  }

  printf("Conectado ao servidor com sucesso.\n");
  

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};  // Array de strings para armazenar chaves 
  unsigned int delay_ms;
  size_t num;

  while (1) {
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
        // FIXX MEEE: end notifications thread
        printf("Disconnected from server\n");
        return 0;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE); // parse_list lê uma lista de argumentos do stdin e armazena em keys.
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        } 
        if (kvs_subscribe(keys[0])) {  //Chama a função kvs_subscribe para subscrever à chave lida
            fprintf(stderr, "Command subscribe failed\n");
        }
        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        } 
        if (kvs_unsubscribe(keys[0])) {  // Chama kvs_unsubscribe para cancelar a subscrição da chave especificada
          fprintf(stderr, "Command subscribe failed\n");
        }
        break;

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) { //  lê um valor de atraso do stdin e armazena em delay_ms.
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

      case EOC: //input acaba em EOC??
        // input should end in a disconnect, or it will loop here forever
        break;
    }
  }
}
