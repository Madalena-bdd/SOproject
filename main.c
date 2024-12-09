#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <sys/wait.h>  
#include <signal.h>    
#include <errno.h>  
#include "constants.h"
#include "parser.h"
#include "operations.h"

#define MAX_FILES 100

int MAX_THREADS = 0; // Número máximo de threads
int concurrent_backups = 0;
int running_backups = 0;  // Number of backups currently running

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
             (int)(strlen(filename) - 4), filename, backup_num);

    // Abrir arquivo de backup
    int backup_fd = open(backup_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (backup_fd == -1) {
        perror("Failed to create backup file");
        exit(EXIT_FAILURE);
    } else {
        fprintf(stderr, "Backup file created: %s\n", backup_filename);
    }

    // Executar a operação de backup
    if (kvs_backup(backup_fd) != 0) {
        fprintf(stderr, "Failed to write backup to %s\n", backup_filename);
        exit(EXIT_FAILURE);
    }

    close(backup_fd);
    exit(EXIT_SUCCESS); // Finaliza o processo filho
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
int process_directory(const char *dirpath) {
    DIR *dir = opendir(dirpath);
    if (!dir) {
        perror("Failed to open directory");
        return -1;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        char filepath[MAX_JOB_FILE_NAME_SIZE];
        snprintf(filepath, sizeof(filepath), "%s/%s", dirpath, entry->d_name);

        struct stat file_stat;
        if (stat(filepath, &file_stat) == 0 && S_ISREG(file_stat.st_mode) && strstr(entry->d_name, ".job")) {
            // Processar cada arquivo em um processo filho separado
            pid_t pid = fork();
            if (pid == 0) {
                // Processo filho
                if (process_job_file(filepath) != 0) {
                    fprintf(stderr, "Error processing file: %s\n", filepath);
                }
                exit(0);
            } else if (pid < 0) {
                perror("Failed to fork process for file processing");
            }
        }
    }

    // Esperar que todos os processos filhos terminem
    while (wait(NULL) > 0);

    closedir(dir);
    return 0;
}


int main(int argc, char *argv[]) {
    if (argc != 4) { 
        fprintf(stderr, "Usage: %s <directory_path> <concurrent_backups>\n", argv[0]);
        return 1;
    }

    const char *dirpath = argv[1];

    concurrent_backups = atoi(argv[2]);
    MAX_THREADS = atoi(argv[3]);
    if (concurrent_backups <= 0 || MAX_THREADS <=0) { 
        fprintf(stderr, "Error: <concurrent_backups> must be greater than 0\n");
        return 1;
    }


    signal(SIGCHLD, handle_sigchld);  // Configura manipulador de sinal para SIGCHLD

    if (kvs_init()) {
        perror("Failed to initialize KVS");
        return 1;
    }

    if (process_directory(dirpath) != 0) {
        kvs_terminate();
        return 1;
    }
    
    // Esperar por todos os processos filhos finalizarem
    while (running_backups > 0) {
        waitpid(-1, NULL, 0);
    }

    kvs_terminate();
    return 0;
}
