#include "api.h"
#include <ctype.h>
#include <dirent.h>
#include <errno.h>  
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>    
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>  
#include <unistd.h>

static char g_req_pipe_path[PATH_MAX];
static char g_resp_pipe_path[PATH_MAX];
static char g_notif_pipe_path[PATH_MAX];



int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path, int* notif_pipe) { // fix me - os tamanhos das mensagens e como estão a ser lançadas

    // Guardar os paths em variáveis globais
    strncpy(g_req_pipe_path, req_pipe_path, PATH_MAX - 1);
    g_req_pipe_path[PATH_MAX - 1] = '\0'; // Garantir terminação nula
    strncpy(g_resp_pipe_path, resp_pipe_path, PATH_MAX - 1);
    g_resp_pipe_path[PATH_MAX - 1] = '\0';
    strncpy(g_notif_pipe_path, notif_pipe_path, PATH_MAX - 1);
    g_notif_pipe_path[PATH_MAX - 1] = '\0';

    // Criar os FIFOs, caso não existam
    if (mkfifo(req_pipe_path, 0666) == -1 && errno != EEXIST) {
        perror("Erro ao criar FIFO de pedidos");
        return 1;
    }

    if (mkfifo(resp_pipe_path, 0666) == -1 && errno != EEXIST) {
        perror("Erro ao criar FIFO de respostas");
        return 1;
    }

    if (mkfifo(notif_pipe_path, 0666) == -1 && errno != EEXIST) {
        perror("Erro ao criar FIFO de notificações");
        return 1;
    }

  // Abrir os FIFOs criados pelo cliente
    int req_pipe = open(req_pipe_path, O_WRONLY);
    if (req_pipe == -1) {
        perror("Erro ao abrir FIFO de pedidos");
        return 1;
    }

    int resp_pipe = open(resp_pipe_path, O_RDONLY);
    if (resp_pipe == -1) {
        perror("Erro ao abrir FIFO de respostas");
        close(req_pipe);
        return 1;
    }

    // Abrir o FIFO de notificações para leitura
    *notif_pipe = open(notif_pipe_path, O_RDONLY);
    if (*notif_pipe == -1) {
        perror("Erro ao abrir FIFO de notificações");
        close(req_pipe);
        close(resp_pipe);
        return 1;
    }

    // Abrir o FIFO do servidor (pré-criado pelo servidor)
    int server_pipe = open(server_pipe_path, O_WRONLY);
    if (server_pipe == -1) {
        perror("Erro ao conectar ao servidor");
        close(req_pipe);
        close(resp_pipe);
        close(*notif_pipe);
        return 1;
    }

    // Criar a mensagem de pedido
    char message[MAX_PIPE_PATH_LENGTH * 3 + 3 + sizeof(int)];  
    snprintf(message, sizeof(message), "1|%s|%s|%s", req_pipe_path, resp_pipe_path, notif_pipe_path);

    // Enviar o pedido para o servidor
    if (write(server_pipe, message, strlen(message)) == -1) {
        perror("Erro ao enviar pedido ao servidor");
        close(req_pipe);
        close(resp_pipe);
        close(*notif_pipe);
        close(server_pipe);
        return 1;
    }

    // Esperar resposta do servidor
    char response[2];  // Espera um código de resposta do servidor
    if (read(resp_pipe, response, sizeof(response)) == -1) {
        perror("Erro ao ler resposta do servidor");
        close(req_pipe);
        close(resp_pipe);
        close(*notif_pipe);
        close(server_pipe);
        return 1;
    }

    // Imprimir mensagem formatada no stdout
    printf("Server returned %c for operation: connect\n", response[0]);

    // Fechar todos os FIFOs ao finalizar
    close(req_pipe);
    close(resp_pipe);
    close(*notif_pipe);
    close(server_pipe);
    return 0;
  }

 
int kvs_disconnect(void) {

    // Abrir o FIFO de pedidos para enviar o pedido de desconexão
    int req_pipe_fd = open(g_req_pipe_path, O_WRONLY);
    if (req_pipe_fd == -1) {
        perror("Erro ao abrir FIFO de pedidos");
        return 1;
    }

        // Criar e enviar a mensagem de desconexão
    char message[MAX_PIPE_PATH_LENGTH * 3 + 3 + sizeof(int)];  
    snprintf(message, sizeof(message), "2|%s|%s|%s", g_req_pipe_path, g_resp_pipe_path, g_notif_pipe_path);
    if (write(req_pipe_fd, message, strlen(message)) == -1) {
        perror("Erro ao enviar pedido de desconexão");
        close(req_pipe_fd);
        return 1;
    }

    // Abrir o FIFO de respostas para ler a resposta do servidor
    int resp_pipe_fd = open(g_resp_pipe_path, O_RDONLY);
    if (resp_pipe_fd == -1) {
        perror("Erro ao abrir FIFO de respostas");
        close(req_pipe_fd);
        return 1;
    }

    // Ler a resposta do servidor
    char response[2];
    if (read(resp_pipe_fd, response, sizeof(response)) == -1) {
        perror("Erro ao ler resposta do servidor");
        close(req_pipe_fd);
        close(resp_pipe_fd);
        return 1;
    }

    // Imprimir mensagem formatada no stdout
    printf("Server returned %c for operation: disconnect\n", response[0]);

    // Fechar os FIFOs abertos
    close(req_pipe_fd);
    close(resp_pipe_fd);

    // Remover os ficheiros FIFO do cliente
    if (unlink(g_req_pipe_path) == -1) {
        perror("Erro ao remover FIFO de pedidos");
        return 1;
    }

    if (unlink(g_resp_pipe_path) == -1) {
        perror("Erro ao remover FIFO de respostas");
        return 1;
    }

    if (unlink(g_notif_pipe_path) == -1) {
        perror("Erro ao remover FIFO de notificações");
        return 1;
    }
  
  return 0;
}
int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
    if (key == NULL) {
    fprintf(stderr, "Erro: Chave nula na subscrição.\n");
    return 1;
    }

    // Abrir o FIFO de pedidos para enviar o pedido de subscrição
    int req_pipe_fd = open(g_req_pipe_path, O_WRONLY);
    if (req_pipe_fd == -1) {
        perror("Erro ao abrir FIFO de pedidos");
        return 1;
    }

    // Criar a mensagem de subscrição (formato: "3|chave")
    char message[MAX_PIPE_PATH_LENGTH * 3 + 3 + sizeof(int)];
    snprintf(message, sizeof(message), "3|%s", key);

        // Abrir o FIFO de respostas para ler a resposta do servidor
    int resp_pipe_fd = open(g_resp_pipe_path, O_RDONLY);
    if (resp_pipe_fd == -1) {
        perror("Erro ao abrir FIFO de respostas");
        close(req_pipe_fd);
        return 1;
    }

    // Ler a resposta do servidor (espera-se um único caractere: '0' ou '1')
    char response[2];
    if (read(resp_pipe_fd, response, sizeof(response)) == -1) {
        perror("Erro ao ler resposta do servidor");
        close(req_pipe_fd);
        close(resp_pipe_fd);
        return 1;
    }

    printf("Server returned %c for operation: subscribe\n", response[0]);

    // Fechar os FIFOs abertos
    close(req_pipe_fd);
    close(resp_pipe_fd);

  return 0;
}

int kvs_unsubscribe(const char* key) {
    // Verificar se a chave é válida
    if (key == NULL) {
        fprintf(stderr, "Erro: Chave nula na desubscrição.\n");
        return 1;
    }

    // Abrir o FIFO de pedidos para enviar o pedido de desubscrição
    int req_pipe_fd = open(g_req_pipe_path, O_WRONLY);
    if (req_pipe_fd == -1) {
        perror("Erro ao abrir FIFO de pedidos");
        return 1;
    }

    // Criar a mensagem de desubscrição (formato: "4|chave")
    char message[MAX_PIPE_PATH_LENGTH * 3 + 3 + sizeof(int)];
    snprintf(message, sizeof(message), "3|%s", key);

    // Enviar a mensagem para o servidor
    if (write(req_pipe_fd, message, strlen(message)) == -1) {
        perror("Erro ao enviar pedido de desubscrição");
        close(req_pipe_fd);
        return 1;
    }

    // Abrir o FIFO de respostas para ler a resposta do servidor
    int resp_pipe_fd = open(g_resp_pipe_path, O_RDONLY);
    if (resp_pipe_fd == -1) {
        perror("Erro ao abrir FIFO de respostas");
        close(req_pipe_fd);
        return 1;
    }

    // Ler a resposta do servidor (espera-se um único caractere: '0' ou '1')
    char response[2];
    if (read(resp_pipe_fd, response, sizeof(response)) == -1) {
        perror("Erro ao ler resposta do servidor");
        close(req_pipe_fd);
        close(resp_pipe_fd);
        return 1;
    }

    // Imprimir a resposta do servidor
    printf("Server returned %c for operation: unsubscribe\n", response[0]);

    // Fechar os FIFOs abertos
    close(req_pipe_fd);
    close(resp_pipe_fd);

    return 0;
}