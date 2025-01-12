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
    struct Cliente* next;
} Cliente;

Cliente* clients = NULL; 

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t sessions_lock = PTHREAD_MUTEX_INITIALIZER;

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

  // Terminar o KVS
  kvs_terminate();

  return 0;
}



//Sugestão: Passar para o main.c do cliente


// ------PEDIDOS CLIENTE (SUBSCRIBE, UNSUBSCRIBE, DISCONNECT------
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
    if (strcmp(command_type, "SUBSCRIBE") == 0) {
        handle_subscribe(client_id, argument);
    } else if (strcmp(command_type, "UNSUBSCRIBE") == 0) {
        handle_unsubscribe(client_id, argument);
    } else if (strcmp(command_type, "DISCONNECT") == 0) {
        //handle_disconnect(client_id);
    } else {
        fprintf(stderr, "Comando desconhecido: %s\n", command_type);
    }
}


/*
problemas:
-qual é "command" que está a receber?
-ele está a divir o comando em comnado e argumento ou seja o comando é algo como "SUBSCRIBE chave"
-função não está definida
-funções handle_subscribe, handle_unsubscribe e handle_disconnect não existem
-só há estes três comandos que o cliente pode pedir?
*/

// ------PEDIDOS DE CONEXÃO DE NOVOS CLIENTE + CRIAÇÃO DO CLIENTE------
int handle_connection_request(int server_pipe_fd, char* request_message, int client_id) {
    char response[2]; // Armazena a resposta a ser enviada (Sucesso ou Erro)

    // Resposta = Erro
    // Bloquear a execução até que haja espaço para uma nova sessão
    pthread_mutex_lock(&sessions_lock);
    if (active_sessions >= MAX_SESSIONS) {
        // Se o servidor não puder aceitar mais sessões, enviar erro e desbloquear
        response[0] = '0';  // Código de erro (máximo de sessões atingido)
        write(server_pipe_fd, response, sizeof(response));
        pthread_mutex_unlock(&sessions_lock);
        return 1;  // Retornar erro
    }

    // Resposta = Sucesso
    // Se houver espaço, criar a nova sessão
    active_sessions++;
    response[0] = '1';  // Confirmação de sucesso

    // Enviar resposta ao cliente
    write(server_pipe_fd, response, sizeof(response));
    pthread_mutex_unlock(&sessions_lock);


    // ---Criar um novo cliente e adicionar à lista---

    // Extrair o ID do cliente e os caminhos dos FIFOs da mensagem
    //int client_id ; //FIX MEE: variável não é inicialiizada; Sugestão: não é melhor colocar no main.c do cliente para fazer apenas arg[1] do id ?
    char fifo_request[MAX_STRING_SIZE], fifo_response[MAX_STRING_SIZE], fifo_notify[MAX_STRING_SIZE];

    // A mensagem tem o formato: "1|<fifo_request>|<fifo_response>|<fifo_notify>"
    sscanf(request_message, "1|%[^|]|%[^|]|%s", fifo_request, fifo_response, fifo_notify);

    // Criar novo cliente
    Cliente* novo_cliente = (Cliente*)malloc(sizeof(Cliente));
    if (novo_cliente == NULL) {
        perror("Erro ao alocar memória para novo cliente");
        return -1;
    }

    // Inicializar o cliente (estrutura)
    novo_cliente->id = client_id;  // ID poderia ser gerado ou passado, dependendo da implementação
    strncpy(novo_cliente->fifo_request, fifo_request, MAX_STRING_SIZE);
    strncpy(novo_cliente->fifo_response, fifo_response, MAX_STRING_SIZE);
    strncpy(novo_cliente->fifo_notify, fifo_notify, MAX_STRING_SIZE);
    memset(novo_cliente->chaves_subscritas, 0, sizeof(novo_cliente->chaves_subscritas)); //inicializar todas as chaves a 0
    novo_cliente->next = NULL;

    // Adicionar cliente à lista ligada
    pthread_mutex_lock(&sessions_lock);
    novo_cliente->next = clients;
    clients = novo_cliente;
    pthread_mutex_unlock(&sessions_lock);

    return 0; 
}
/*
problemas:
-está a enviar a resposta (l. 522) e a mensagem (l.534) (não está a enviar o "1" duas vezes??)
-l.547
*/

//MANTÉM O PROBLEMA DO ID DO CLIENTE
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

//MANTÉM O PROBLEMA DO ID DO CLIENTE
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