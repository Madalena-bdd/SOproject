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
#include "constants.h"
#include "parser.h"
#include "operations.h"

#define MAX_FILES 100

int concurrent_backups = 0;

// Função para processar arquivos .job
int process_job_file(const char *filename) {
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open file");
        return -1;
    }

    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    // Criar o nome do arquivo de saída
    char output_filename[MAX_JOB_FILE_NAME_SIZE];
    snprintf(output_filename, sizeof(output_filename), "%.*s.out", (int)(strlen(filename) - 4), filename);

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
                num_pairs = (size_t)parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    //dprintf(output_fd, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_write(num_pairs, keys, values)) {
                    //dprintf(output_fd, "Failed to write pair\n");
                }
                break;

            case CMD_READ:
                num_pairs = (size_t)parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    //dprintf(output_fd, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_read(num_pairs, keys, output_fd)) {
                    //dprintf(output_fd, "Failed to read pair\n");
                }
                break;

            case CMD_DELETE:
                num_pairs = (size_t)parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    //dprintf(output_fd, "Invalid command. See HELP for usage\n");
                    continue;
                }
                //if (kvs_delete(num_pairs, keys, output_fd)) {
                    //dprintf(output_fd, "Failed to delete pair\n");
                //}
                kvs_delete(num_pairs, keys, output_fd);
                break;

            case CMD_SHOW:
                kvs_show(output_fd);
                break;

            case CMD_WAIT:
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
                if (kvs_backup()) {
                    //dprintf(output_fd, "Failed to perform backup.\n");
                }
                break;

            case CMD_INVALID:
                //dprintf(output_fd, "Invalid command. See HELP for usage\n");
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
            if (process_job_file(filepath) != 0) {
                fprintf(stderr, "Error processing file: %s\n", filepath);
            }
        }
    }

    closedir(dir);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <directory_path> <concurrent_backups>\n", argv[0]);
        return 1;
    }

    const char *dirpath = argv[1];
    concurrent_backups = atoi(argv[2]);

    if (kvs_init()) {
        perror("Failed to initialize KVS");
        return 1;
    }

    if (process_directory(dirpath) != 0) {
        kvs_terminate();
        return 1;
    }

    kvs_terminate();
    return 0;
}
