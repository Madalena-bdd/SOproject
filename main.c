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
#include "constants.h"
#include "parser.h"
#include "operations.h"

#define MAX_FILES 100

typedef struct Job_data {
  int fd;
  char *file_path;
  int output_fd;
  int running_backups;
  int concurrent_backups;
  int status; // 0 - não foi processado, 1 - já foi ou está a ser processado
  struct Job_data *next;
} Job_data;

typedef struct {
  Job_data* job_data;
  int num_files;
  pthread_mutex_t mutex;
} File_list;

int MAX_THREADS = 0; // Número máximo de threads
int concurrent_backups = 0;
int running_backups = 0;  // Number of backups currently running

void *thread_for_job(void *arg);
void handle_sigchld(int signo);
void perform_backup(const char *filename, int backup_num);
File_list *process_directory(const char *filename);

int main(int argc, char *argv[]) {
    if (argc != 4) { 
        fprintf(stderr, "Usage: %s <directory_path> <concurrent_backups> <max_threads>\n", argv[0]);
        return 1;
    }

    const char *dirpath = argv[1];
    concurrent_backups = atoi(argv[2]);
    MAX_THREADS = atoi(argv[3]);

    if (concurrent_backups <= 0 || MAX_THREADS <=0) { 
        fprintf(stderr, "Error: <concurrent_backups> must be greater than 0\n");
        return 1;
    }

    if (kvs_init()) {
        perror("Failed to initialize KVS");
        return 1;
    }

    signal(SIGCHLD, handle_sigchld);  // Configura manipulador de sinal para SIGCHLD

    File_list *file_list = process_directory(dirpath);

    if (file_list == NULL) {
        return 1;
    }

    Job_data *job_data = file_list->job_data;
    pthread_t threads[MAX_THREADS];
    int num_files = file_list->num_files;

    for (int i = 0; i < MAX_THREADS && i < num_files; i++) {
        pthread_create(&threads[i], NULL, thread_for_job, (void *)file_list);
        job_data = job_data->next;
    }

    for (int i = 0; i < MAX_THREADS && i < num_files; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Esperar por todos os processos filhos finalizarem
    while (running_backups > 0) {
        pid_t pid = waitpid(-1, NULL, 0);
        if (pid > 0) {
            __sync_fetch_and_sub(&running_backups, 1); // Decrementa atomicamente
        } else if (pid == -1 && errno == ECHILD) {
            // Sem mais processos filhos, pode sair do loop
            break;
        }
    }

    // Clear the file_list
    Job_data *current_job = file_list->job_data;
    while (current_job != NULL) {
        Job_data *next_job = current_job->next;
        free(current_job->file_path);
        free(current_job);
        current_job = next_job;
    }
    free(file_list);
    kvs_terminate();
    return 0;
}

// Recolher processos filhos finalizados
void handle_sigchld(int signo) {
    (void)signo; // Marca o parametro como usado
    while (waitpid(-1, NULL, WNOHANG) > 0) {
        __sync_fetch_and_sub(&running_backups, 1); // Decrementa atomicamente o contador
    }
}

// Função para realizar o backup em um processo filho
void perform_backup(const char *filename, int backup_num) {
    // Criar o nome do arquivo de backup com o número do backup
    char backup_filename[MAX_JOB_FILE_NAME_SIZE];
    snprintf(backup_filename, sizeof(backup_filename), "%.*s-%d.bck",
        (int)(strlen(filename) - 4), filename, backup_num+1);

    // Abrir arquivo de backup
    int backup_fd = open(backup_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (backup_fd == -1) {
        perror("Failed to create backup file");
        return;
    } else {
        fprintf(stderr, "Backup file created: %s\n", backup_filename);
    }

    // Executar a operação de backup
    if (kvs_backup(backup_fd) != 0) {
        fprintf(stderr, "Failed to write backup to %s\n", backup_filename);
        close(backup_fd);
        return;
    }

    close(backup_fd);
}

// Função para processar arquivos .job
int process_job_file(const char *filename) {
    int backup_count = 0;  // Contador de backups por arquivo .job

    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open file");
        return -1;
    }

    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    // Criar o nome do arquivo de saída (inclui o PID para exclusividade)
    char output_filename[MAX_JOB_FILE_NAME_SIZE];
    snprintf(output_filename, sizeof(output_filename), "%.*s.out",
        (int)(strlen(filename) - 4), filename);

    int output_fd = open(output_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (output_fd == -1) {
        perror("Failed to open output file");
        close(fd);
        return -1;
    }

    enum Command command;
    while ((command = get_next(fd)) != EOC) {
        switch (command) {
            case CMD_WRITE:
                //fprintf(stderr, "Executing command: CMD_WRITE\n"); // DEBUG
                num_pairs = (size_t)parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_write(num_pairs, keys, values)) {
                    fprintf(stderr, "Failed to write pair\n");
                }
                break;

            case CMD_READ:
                //fprintf(stderr, "Executing command: CMD_READ\n");  // DEBUG
                num_pairs = (size_t)parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_read(num_pairs, keys, output_fd)) {
                    fprintf(stderr, "Failed to read pair\n");
                }
                break;

            case CMD_DELETE:
                //fprintf(stderr, "Executing command: CMD_DELETE\n"); // DEBUG
                num_pairs = (size_t)parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                //if (kvs_delete(num_pairs, keys, output_fd)) {
                    //dprintf(output_fd, "Failed to delete pair\n");
                //}
                kvs_delete(num_pairs, keys, output_fd);
                break;

            case CMD_SHOW:
                //fprintf(stderr, "Executing command: CMD_SHOW\n"); // DEBUG
                kvs_show(output_fd);
                break;

            case CMD_WAIT:
                //fprintf(stderr, "Executing command: CMD_WAIT\n"); // DEBUG
                if (parse_wait(fd, &delay, NULL) == -1) {
                    //dprintf(output_fd, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (delay > 0) {
                    //dprintf(output_fd, "Waiting...\n");
                    kvs_wait(delay); 
                }
                break;

            case CMD_BACKUP:
                //fprintf(stderr, "Executing command: CMD_BACKUP\n"); // DEBUG
                kvs_wait_backup(filename, &backup_count);

                //if (kvs_backup()) {
                    //dprintf(output_fd, "Failed to perform backup.\n");
                //}
                break;

            case CMD_INVALID:
                //fprintf(stderr, "Executing command: CMD_INVALID\n"); // DEBUG
                dprintf(output_fd, "Invalid command. See HELP for usage\n");
                break;

            case CMD_HELP:
                dprintf(output_fd,
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n"
                    "  HELP\n"
                );
                break;

            case CMD_EMPTY:
                break;
            case EOC:
                kvs_terminate();
                return 0;
        }
    }
    close(fd);
    close(output_fd);
    return 0;
}

// Função para processar arquivos em um diretório
File_list *process_directory(const char *dirpath) {
    DIR *dir = opendir(dirpath);

    if (dir == NULL) {
        perror("Unable to open directory");
        return NULL;
    }

    struct dirent *entry;

    File_list *file_list = (File_list *)malloc(sizeof(File_list));

    if (file_list == NULL) {
        perror("Failed to allocate memory for file list");
        return NULL;
    }

    file_list->num_files = 0;

    while ((entry = readdir(dir)) != NULL) {
        char filepath[MAX_JOB_FILE_NAME_SIZE];
        if ((size_t)snprintf(filepath, sizeof(filepath), "%s/%s", dirpath, entry->d_name) >= sizeof(filepath)) {
            fprintf(stderr, "Error: File path is too long: %s/%s\n", dirpath, entry->d_name);
            continue;
        }

        struct stat file_metadata;
        if (stat(filepath, &file_metadata) == 0 && S_ISREG(file_metadata.st_mode) && strstr(entry->d_name, ".job")) {
            Job_data *job_data = (Job_data *)malloc(sizeof(Job_data));
            job_data->file_path = strdup(filepath);
            job_data->running_backups = 0;
            job_data->concurrent_backups = concurrent_backups;
            job_data->status = 0;
            job_data->next = NULL;
            if (file_list->job_data == NULL) { 
                file_list->job_data = job_data;
            } else {
                Job_data* current_job = file_list->job_data;
                while (current_job->next != NULL) {
                    current_job = current_job->next;
                }
                current_job->next = job_data;
                job_data->next = NULL;
            }
            file_list->num_files++;
        }
    }
    closedir(dir);
    return file_list;
}

void *thread_for_job(void *arg) {
    File_list *file_list = (File_list *)arg;
    Job_data *job_data = file_list->job_data;
    for (; job_data != NULL; job_data = job_data->next) {
        pthread_mutex_lock(&file_list->mutex);
        if (job_data->status == 0) {
            job_data->status = 1;
            process_job_file(job_data->file_path);
            pthread_mutex_unlock(&file_list->mutex);
            
        } else {
            pthread_mutex_unlock(&file_list->mutex);
        }
    }
    return NULL;
}
