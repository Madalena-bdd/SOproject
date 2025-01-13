#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>    
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

typedef struct Cliente {
  int id;  // ID do processo cliente
  char fifo_request[MAX_STRING_SIZE];   // Caminho do FIFO de pedidos
  char fifo_response[MAX_STRING_SIZE];  // Caminho do FIFO de respostas
  char fifo_notify[MAX_STRING_SIZE];    // Caminho do FIFO de notificações
  char chaves_subscritas[MAX_KEYS][MAX_STRING_SIZE];  // Chaves subscritas
  pthread_t thread_id;  // Thread ID for the client handler
  int req_pipe_fd;  // File descriptor for request FIFO
  int resp_pipe_fd; // File descriptor for response FIFO
  int notif_pipe_fd; // File descriptor for notification FIFO
  struct Cliente* next;
} Cliente;

Cliente* clients = NULL; 

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t sessions_lock = PTHREAD_MUTEX_INITIALIZER;
//Esperar por Desconexões
int active_threads = 0;
pthread_mutex_t thread_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t thread_cond = PTHREAD_COND_INITIALIZER;
pthread_t connection_manager_thread;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
size_t active_sessions = 0;    // Number of active sessions

char* jobs_directory = NULL;
char* registration_fifo_name_global = NULL; // Global variable for the FIFO name //cainho do fifo de registo!
char active_sessions_list[MAX_SESSIONS][PATH_MAX]; // LISTA DE SESSÕES ATIVAS

// Declarações das funções
int handle_subscribe(int client_id, const char* key);
int handle_unsubscribe(int client_id, const char* key);
void* client_handler(void* arg);
void* connection_manager(void* arg);
void handle_connect(Cliente* novo_cliente);
void handle_connection_requests(int req_fd);


// ------JOBS------ (1ªentrega)

int filter_job_files(const struct dirent* entry) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot != NULL && strcmp(dot, ".job") == 0) {
        return 1;  // Keep this file (it has the .job extension)
    }
    return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting %d seconds\n", delay / 1000);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        pthread_mutex_lock(&n_current_backups_lock);
        if (active_backups >= max_backups) {
          wait(NULL);
        } else {
          active_backups++;
        }
        pthread_mutex_unlock(&n_current_backups_lock);
        int aux = kvs_backup(++file_backups, filename, jobs_directory);

        if (aux < 0) {
            write_str(STDERR_FILENO, "Failed to do backup\n");
        } else if (aux == 1) {
          return 1;
        }
        break;

      case CMD_INVALID:
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write_str(STDOUT_FILENO,
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        printf("EOF\n");
        return 0;
    }
  }
}

//frees arguments
static void* get_file(void* arguments) {
  struct SharedData* thread_data = (struct SharedData*) arguments;
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

// ------SESSIONS------

// Função para ADICIONAR UMA SESSÃO
int add_session(const char* client_pipe_path) {
    pthread_mutex_lock(&sessions_lock); // Bloqueia o mutex para garantir que a seção crítica seja acessada por apenas uma thread de cada vez
    if (active_sessions >= MAX_SESSIONS) {
        pthread_mutex_unlock(&sessions_lock);
        return -1; // Erro: Máximo de sessões atingido
    }
    strncpy(active_sessions_list[active_sessions], client_pipe_path, PATH_MAX - 1);
    active_sessions++;
    pthread_mutex_unlock(&sessions_lock);
    return 0;
}

// Função para REMOVER UMA SESSÃO
int remove_session(const char* client_pipe_path) {
    pthread_mutex_lock(&sessions_lock);
    for (size_t i = 0; i < active_sessions; i++) {
        if (strcmp(active_sessions_list[i], client_pipe_path) == 0) {
            // Remover a sessão deslocando as demais
            for (size_t j = i; j < active_sessions - 1; j++) {
                strncpy(active_sessions_list[j], active_sessions_list[j + 1], PATH_MAX);
            }
            active_sessions--;
            pthread_mutex_unlock(&sessions_lock);
            return 0; // Sucesso
        }
    }
    pthread_mutex_unlock(&sessions_lock);
    fprintf(stderr, "Session not found.\n");
    return -1; // Seção não encontrada
}

// Função para verificar se uma SESSÃO ESTÁ ATIVA?
int is_session_active(const char* client_pipe_path) {
    pthread_mutex_lock(&sessions_lock);
    for (size_t i = 0; i < active_sessions; i++) {
        if (strcmp(active_sessions_list[i], client_pipe_path) == 0) {
            pthread_mutex_unlock(&sessions_lock);
            fprintf(stderr, "Session active.\n");
            return 1; // Sessão ativa
        }
    }
    pthread_mutex_unlock(&sessions_lock);
    fprintf(stderr, "Session not active.\n");
    return 0; // Sessão não ativa
}

// ------MÚLTIPLAS THREADS------

static void dispatch_threads(DIR* dir) {

  // Aloca memória no array pthread_t que armazena os identificadores das threads
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));
  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER}; // Inicializa a estrutura de dados compartilhados

  // Criação das threads
  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // Ler o FIFO de registo
  int registration_fifo_fd = open(registration_fifo_name_global, O_RDONLY );
  if (registration_fifo_fd == -1) { 
    free(threads);
    return;
  }

  char buffer[1024];  // Tamanho do buffer para dados recebidos
  ssize_t bytes_read;
  while ((bytes_read = read(registration_fifo_fd, buffer, sizeof(buffer) - 1)) > 0) {
      buffer[bytes_read] = '\0';  // Garantir que a string seja terminada corretamente
      printf("Received from FIFO: %s\n", buffer);
  }

  if (bytes_read == -1) {
      perror("Error reading from FIFO");
  }
  close(registration_fifo_fd);


  // Aguarda a conclusão das threads
  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // Destruição do mutex e libertação de memória
  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }
  free(threads);
}

// ------LIMPAR FIFO------

// Função para limpar o FIFO ao sair
void cleanup_fifo_server() {
    if (access(registration_fifo_name_global, F_OK) != -1) { // Verifica se o FIFO existe
        if (unlink(registration_fifo_name_global) == -1) {
            perror("Failed to unlink FIFO");
        } else {
            fprintf(stdout, "FIFO %s removed\n", registration_fifo_name_global);
        }
    } else {
        fprintf(stdout, "FIFO %s does not exist, skipping removal.\n", registration_fifo_name_global);
    }
    pthread_cancel(connection_manager_thread);
}


// ------MAIN------
int main(int argc, char** argv) {
  if (argc < 5) {  //TESTAR com 6 argumentos!!!
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups> \n");
    write_str(STDERR_FILENO, " <registration_fifo_name_global> \n");
    return 1;
  }

  jobs_directory = argv[1];  // Define o argumento 1 como o diretório de jobs
  char* endptr;

  max_threads = strtoul(argv[2], &endptr, 10);  // Define o argumento 2 como o número máximo de threads
  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  max_backups = strtoul(argv[3], &endptr, 10);  // Define o argumento 3 como o número máximo de backups
  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_backups value\n");
    return 1;
  }
  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }
  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  registration_fifo_name_global = argv[4];  // Define o argumento 4 como o FIFO de registo

  // Criar o FIFO de registro com o caminho de registo_fifo_name_global
  if (mkfifo(registration_fifo_name_global, 0666) == -1) {
    if (errno != EEXIST) {  // Ignorar erro se o FIFO já existir
      perror("Failed to create registration FIFO");
      return 1;
    }
  }
  fprintf(stdout, "Registration FIFO created: %s\n", registration_fifo_name_global);

  // Registra a função de limpeza do FIFO para ser chamada ao sair do programa
  // Quando o programa recebe um sinal SIGINT (Ctrl+C), a função cleanup_fifo_server é chamada
  signal(SIGINT, cleanup_fifo_server);

  // Inicializa o KVS (armazenamento de chave-valor)
  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  // Abrir o FIFO de conexão do servidor
  int server_fifo_fd = open(registration_fifo_name_global, O_RDONLY);
  if (server_fifo_fd == -1) {
    perror("Failed to open registration FIFO");
    return 1;
  }

  // Criar a thread do gerenciador de conexões
  if (pthread_create(&connection_manager_thread, NULL, connection_manager, &server_fifo_fd) != 0) {
    perror("Failed to create connection manager thread");
    close(server_fifo_fd);
    return 1;
  }

  // Abre o diretório de jobs
  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  // Criar e gerenciar múltiplas threads que processarão os arquivos no diretório
  dispatch_threads(dir);

  // Fecha o diretório
  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  // Aguardar conclusão de backups ativos
  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  // Aguardar a conclusão da thread do gerenciador de conexões
  if (pthread_join(connection_manager_thread, NULL) != 0) {
    perror("Failed to join connection manager thread");
    close(server_fifo_fd);
    return 1;
  }

  // Fechar o FIFO de conexão do servidor
  close(server_fifo_fd);

  // Terminar o KVS
  kvs_terminate();

  return 0;
}

void* connection_manager(void* arg) {
    int server_fifo_fd = *(int*)arg;
    char buffer[256];

    while (1) {
        ssize_t bytes_read = read(server_fifo_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read > 0) {
            buffer[bytes_read] = '\0'; // Garante terminação da string
            char command[2], req_path[256], resp_path[256], notif_path[256];
            sscanf(buffer, "%1s|%255[^|]|%255[^|]|%255s", command, req_path, resp_path, notif_path);
            
            if (strcmp(command, "1") == 0) { 
                // Verificação se há espaço para nova sessão
                char response[2];  // Armazena a resposta a ser enviada (Sucesso ou Erro)

                pthread_mutex_lock(&sessions_lock);
                if (active_sessions >= MAX_SESSIONS) {
                    // Se o servidor não puder aceitar mais sessões, enviar erro e desbloquear
                    response[0] = '0';  // Código de erro (máximo de sessões atingido)
                    write(server_fifo_fd, response, sizeof(response));
                    pthread_mutex_unlock(&sessions_lock);
                    continue;
                }

                // Se houver espaço, criar a nova sessão
                active_sessions++;
                response[0] = '1';  // Confirmação de sucesso

                // Enviar resposta ao cliente
                write(server_fifo_fd, response, sizeof(response));
                pthread_mutex_unlock(&sessions_lock);

                // Criar novo cliente e adicionar à lista de clientes
                Cliente* novo_cliente = (Cliente*)malloc(sizeof(Cliente));
                if (novo_cliente == NULL) {
                    perror("Erro ao alocar memória para novo cliente");
                    continue;
                }

                char* client_id = strrchr(req_path, 'q');
                novo_cliente->id = atoi(client_id);  // ID do cliente
                strncpy(novo_cliente->fifo_request, req_path, MAX_STRING_SIZE);
                strncpy(novo_cliente->fifo_response, resp_path, MAX_STRING_SIZE);
                strncpy(novo_cliente->fifo_notify, notif_path, MAX_STRING_SIZE);
                memset(novo_cliente->chaves_subscritas, 0, sizeof(novo_cliente->chaves_subscritas)); // Inicializa as chaves como 0
                novo_cliente->next = NULL;

                // Adicionar o cliente à lista ligada
                pthread_mutex_lock(&sessions_lock);
                novo_cliente->next = clients;
                clients = novo_cliente;
                pthread_mutex_unlock(&sessions_lock);

                // Call handle_connect to handle the connection
                handle_connect(novo_cliente);
            }
        }
    }
    return NULL;
}


void handle_connect(Cliente* novo_cliente) {
    int req_fd = open(novo_cliente->fifo_request, O_RDONLY);
    int resp_fd = open(novo_cliente->fifo_response, O_WRONLY);
    int notif_fd = open(novo_cliente->fifo_notify, O_WRONLY);

    if (req_fd == -1 || resp_fd == -1 || notif_fd == -1) {
        perror("Erro ao abrir FIFOs do cliente");
        // Responder ao cliente com erro
        char error_message[] = "0"; // erro = 0
        write(resp_fd, error_message, strlen(error_message));
        if (req_fd != -1) close(req_fd);
        if (resp_fd != -1) close(resp_fd);
        if (notif_fd != -1) close(notif_fd);
        return;
    }

    // Responder ao cliente com sucesso
    char success_message[] = "1"; // sucesso = 1
    write(resp_fd, success_message, strlen(success_message));

    // Passar os descritores para uma thread dedicada
    novo_cliente->req_pipe_fd = req_fd;
    novo_cliente->resp_pipe_fd = resp_fd;
    novo_cliente->notif_pipe_fd = notif_fd;

    pthread_t client_thread;
    pthread_create(&client_thread, NULL, client_handler, novo_cliente);
}


// ------PEDIDOS CLIENTE (SUBSCRIBE, UNSUBSCRIBE, DISCONNECT)------
// Função principal para lidar com os pedidos do cliente
void handle_client_request(int client_id, const char* command) { // o connect é enviado diretamente para o servidor
    char command_type[MAX_STRING_SIZE]; // Armazena o tipo de comando (SUBSCRIBE, UNSUBSCRIBE, DISCONNECT)
    char argument[MAX_STRING_SIZE];  // Armazena o argumento do comando (chave)

    // Dividir o comando em comando e argumento "SUBSCRIBE chave"
    if (sscanf(command, "%s %s", command_type, argument) < 2) {
        fprintf(stderr, "Comando inválido: %s\n", command);
        return;
    }

    // Processamento do comando
    if (strcmp(command_type, "3") == 0) { //SUBSCRIBE==3
        handle_subscribe(client_id, argument);
    } else if (strcmp(command_type, "4") == 0) { //UNSUBSCRIBE==4
        handle_unsubscribe(client_id, argument);
    } else if (strcmp(command_type, "2") == 0) { //DISCONNECT==2
        //handle_disconnect(client_id);
    } else {
        fprintf(stderr, "Comando desconhecido: %s\n", command_type);
    }
}


void* client_handler(void* arg) {
    Cliente* client = (Cliente*)arg;
    char buffer[1024];

    while (1) {
        // Ler o pedido do cliente
        ssize_t n = read(client->req_pipe_fd, buffer, sizeof(buffer));
        if (n <= 0) break;  // Desconexão ou erro

        // Processar o pedido
        if (strncmp(buffer, "2", 10) == 0) { //disconnect == 2
            printf("Cliente %d desconectado\n", client->id);
            break;
        }

        // Responder ao cliente
        write(client->resp_pipe_fd, "0", 1);  // Sucesso
    }

    // Fechar FIFOs
    close(client->req_pipe_fd);
    close(client->resp_pipe_fd);
    close(client->notif_pipe_fd);

    // Remover cliente da lista ligada
    remove_session(client->fifo_request);

    // Liberar memória
    free(client);

    pthread_mutex_lock(&thread_mutex);
    active_threads--;
    pthread_cond_signal(&thread_cond);
    pthread_mutex_unlock(&thread_mutex);

    return NULL;
}

/*
problemas:
-qual é "command" que está a receber?
-ele está a divir o comando em comnado e argumento ou seja o comando é algo como "SUBSCRIBE chave"
-função não está definida
-funções handle_subscribe, handle_unsubscribe e handle_disconnect não existem
-só há estes três comandos que o cliente pode pedir?
*/


int handle_subscribe(int client_id, const char* key) { 
    // Procurar o cliente na lista de clientes ativos usando o ID
    Cliente* cliente = NULL;
    Cliente* current = clients;  // active_clients é a lista de clientes ativos

    // Procurar o cliente pelo ID na lista ligada
    while (current != NULL) {
        if (current->id == client_id) {
            cliente = current;
            break;
        }
        current = current->next;
    }

    if (cliente == NULL) {
        // Cliente não encontrado
        fprintf(stderr, "Cliente não encontrado. ID: %d\n", client_id);
        return 1;  // Erro
    }

     // Verificar se a chave já está subscrita pelo cliente
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (strcmp(cliente->chaves_subscritas[i], key) == 0) {
            return 1;
        }
    }

    // Procurar uma posição livre para armazenar a chave subscrita
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (cliente->chaves_subscritas[i][0] == '\0') { // Se encontrar uma posição livre
            strncpy(cliente->chaves_subscritas[i], key, MAX_STRING_SIZE - 1);
            return 0;
        }
    }

    // Se não houver espaço para mais subscrições
    fprintf(stderr, "Limite de subscrições atingido para o cliente %d\n", client_id);
    return 1;  // Erro
}


int handle_unsubscribe(int client_id, const char* key) {
    // Procurar o cliente na lista de clientes ativos usando o ID
    Cliente* cliente = NULL;
    Cliente* current = clients;  // active_clients é a lista de clientes ativos

    // Procurar o cliente pelo ID na lista ligada
    while (current != NULL) {
        if (current->id == client_id) {
            cliente = current;
            break;
        }
        current = current->next;
    }

    if (cliente == NULL) {
        // Cliente não encontrado
        return 1;  // Erro
    }

    // Verificar se o cliente está subscrito na chave
    int found = 0;  // Flag para verificar se a chave foi encontrada
    for (size_t i = 0; i < MAX_KEYS; i++) {
        if (strcmp(cliente->chaves_subscritas[i], key) == 0) {
            // Encontramos a chave, vamos removê-la
            memset(cliente->chaves_subscritas[i], 0, MAX_STRING_SIZE);  // Limpa a chave
            found = 1;
            break;
        }
    }

    if (!found) {
        // Cliente não está subscrito na chave
        return 1;  // Erro
    }

    return 0;  // Sucesso
}
