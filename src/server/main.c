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

typedef struct Cliente { // fix me - lista de chaves subscritas, não sei se precisa de mais coisas
    int id;  // ID do processo cliente
    char chaves_subscritas[MAX_SESSIONS][MAX_STRING_SIZE];   // Chaves para a qual o cliente está subscrito 
    struct Cliente* next;
} Cliente;

Cliente* clientes = NULL;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
char* jobs_directory = NULL;
char *registration_fifo_name_global; // Variável global para o nome do FIFO
size_t active_sessions = 0;
pthread_mutex_t sessions_lock = PTHREAD_MUTEX_INITIALIZER;
char active_sessions_list[MAX_SESSIONS][PATH_MAX];

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

// Função para adicionar uma sessão
int add_session(const char* client_pipe_path) {
    pthread_mutex_lock(&sessions_lock);
    if (active_sessions >= MAX_SESSIONS) {
        pthread_mutex_unlock(&sessions_lock);
        return -1; // Erro: Máximo de sessões atingido
    }
    strncpy(active_sessions_list[active_sessions], client_pipe_path, PATH_MAX - 1);
    active_sessions++;
    pthread_mutex_unlock(&sessions_lock);
    return 0;
}

// Função para remover uma sessão
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
    return -1; // Sessão não encontrada
}

// Função para verificar se uma sessão está ativa
int is_session_active(const char* client_pipe_path) {
    pthread_mutex_lock(&sessions_lock);
    for (size_t i = 0; i < active_sessions; i++) {
        if (strcmp(active_sessions_list[i], client_pipe_path) == 0) {
            pthread_mutex_unlock(&sessions_lock);
            return 1; // Sessão ativa
        }
    }
    pthread_mutex_unlock(&sessions_lock);
    return 0; // Sessão não ativa
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


static void dispatch_threads(DIR* dir) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};


  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // Ler do FIFO de registo
  int registration_fifo_fd = open(registration_fifo_name_global, O_RDONLY );
  if (registration_fifo_fd == -1) {
    free(threads);
    return;
  }
  // Ler dados do FIFO
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

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}

// Função para limpar o FIFO ao sair
void cleanup_fifo() {
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

int main(int argc, char** argv) {
  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups> \n");
    write_str(STDERR_FILENO, " <registration_fifo_name_global> \n");
    return 1;
  }

  jobs_directory = argv[1];
  char* endptr;
  max_threads = strtoul(argv[2], &endptr, 10);
  max_backups = strtoul(argv[3], &endptr, 10);
  registration_fifo_name_global = argv[4];  //FIFO de registo

  // Criar o FIFO de registro
    if (mkfifo(registration_fifo_name_global, 0666) == -1) {
        if (errno != EEXIST) {                                                      // Ignorar erro se o FIFO já existir
            perror("Failed to create registration FIFO");
            return 1;
        }
    }
    fprintf(stdout, "Registration FIFO created: %s\n", registration_fifo_name_global);

  // Registra a função de limpeza do FIFO para ser chamada ao sair
  signal(SIGINT, cleanup_fifo); 

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
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

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();

  return 0;
}

// Função principal para lidar com os pedidos do cliente
/*void handle_client_request(int client_fd, const char* command) { // o connect é enviado diretamente para o servidor
    char command_type[MAX_STRING_SIZE];
    char argument[MAX_STRING_SIZE];

    // Dividir o comando e o argumento
    if (sscanf(command, "%s %s", command_type, argument) < 2) {
        fprintf(stderr, "Comando inválido: %s\n", command);
        return;
    }

    if (strcmp(command_type, "SUBSCRIBE") == 0) {
        handle_subscribe(client_fd, argument);
    } else if (strcmp(command_type, "UNSUBSCRIBE") == 0) {
        handle_unsubscribe(client_fd, argument);
    } else if (strcmp(command_type, "DISCONNECT") == 0) {
        handle_disconnect(client_fd);
    } else {
        fprintf(stderr, "Comando desconhecido: %s\n", command_type);
        write(client_fd, "ERROR: Comando desconhecido", 26);
    }
}


int handle_connection_request(int server_pipe_fd, char* request_message) {
  char response[2];

      // Bloquear a execução até que haja espaço para uma nova sessão
    pthread_mutex_lock(&sessions_lock);
        if (active_sessions >= MAX_SESSIONS) {
        // Se o servidor não puder aceitar mais sessões, enviar erro e desbloquear
        response[0] = '0';  // Código de erro (máximo de sessões atingido)
        write(server_pipe_fd, response, sizeof(response));
        pthread_mutex_unlock(&sessions_lock);
        return 1;  // Retornar erro, já que o número máximo de sessões foi atingido
    }

        // Se houver espaço, criar a nova sessão
    active_sessions++;
    response[0] = '1'; 

    //FIX ME - criar um novo cliente e adicionar à lista de clientes
    // FIX ME - como é que o cliente recebe a resposta
    // FIX ME - conectar de facto o cliente ao servidor
}*/
