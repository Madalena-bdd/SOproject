/**
 * A program implementing a key-value store (KVS).
 * This file contains the main function of that program.
 * @file main.c
 * @authors:
 *  Madalena Bordadágua - 110382
 *  Madalena Martins - 110698
 */

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
#include "constants.h"
#include "parser.h"
#include "operations.h"

#define MAX_FILES 100

typedef struct Job_data {
  int fd;
  char *file_path;                                                                  // Path to the .job file
  int output_fd;                                                                    // File descriptor for the output
  int running_backups;                                                              // Number of backups currently running for this job
  int concurrent_backups;                                                           // Maximum number of concurrent backups allowed
  int status;                                                                       // 0 - Not processed, 1 - Processed or being processed
  struct Job_data *next;                                                            // Pointer to the next job
} Job_data;

typedef struct {
  Job_data* job_data;                                                               // Pointer to the list of jobs
  int num_files;                                                                    // Number of files in the directory
  pthread_mutex_t mutex;                                                            // Mutex for thread synchronization
} File_list;

int MAX_THREADS = 0;                                                                // Maximum number of threads
volatile int concurrent_backups = 0;                                                         // Maximum number of concurrent backups, received as argument
volatile int running_backups = 0;                                                            // Number of backups currently running globally
//volatiless?
char *registration_fifo_name_global; // Variável global para o nome do FIFO


void *process_jobs_thread(void *arg);
void handle_sigchld(int signo);
void perform_backup(const char *filename, int backup_num);
File_list *process_directory(const char *filename);



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



/// Main function for the program.
/// @param argc The number of command line arguments.
/// @param argv An array of strings containing the command line arguments.
/// @return 0 if the program executed successfully, 1 otherwise.
int main(int argc, char *argv[]) {


    if (argc != 5) { 
        fprintf(stderr, "Usage: %s <directory_path> <concurrent_backups> <max_threads> <registration_fifo_name>\n", argv[0]);
        return 1;
    }

    const char *dirpath = argv[1];
    concurrent_backups = atoi(argv[2]);
    MAX_THREADS = atoi(argv[3]);
    registration_fifo_name_global = argv[4]; // FIFO name


    for (int i = 0; argv[2][i] != '\0'; i++) {
        if (!isdigit(argv[2][i])) {
            fprintf(stderr, "Error: <concurrent_backups> must be a number\n");
            return 1;
        }
    }

    for (int i = 0; argv[3][i] != '\0'; i++) {
        if (!isdigit(argv[3][i])) {
            fprintf(stderr, "Error: <max_threads> must be a number\n");
            return 1;
        }
    }

    if (concurrent_backups <= 0 || MAX_THREADS <= 0) { 
        fprintf(stderr, "Error: <concurrent_backups> must be greater than 0\n");
        return 1;
    }

    if (kvs_init()) {                                                               // Initializes the KVS system
        perror("Failed to initialize KVS");
        return 1;
    }

    signal(SIGCHLD, handle_sigchld);                                                // Set up SIGCHLD handler for handling child process termination

    // Register cleanup for FIFO
    if (mkfifo(registration_fifo_name_global, 0666) == -1) {
        if (errno != EEXIST) {                                                      // Ignore error if FIFO already exists
            perror("Failed to create registration FIFO");
            return 1;
        }
    }
    fprintf(stdout, "Registration FIFO created: %s\n", registration_fifo_name_global);

    int registration_fifo_fd = open(registration_fifo_name_global, O_RDONLY | O_NONBLOCK); // Open FIFO for reading
    if (registration_fifo_fd == -1) {
        perror("Failed to open registration FIFO");
        return 1;
    }

    File_list *file_list = process_directory(dirpath);                              // Process the directory to build the job list
    if (file_list == NULL) {
        close(registration_fifo_fd);
        return 1;
    }

    Job_data *job_data = file_list->job_data;
    pthread_t threads[MAX_THREADS];
    int num_files = file_list->num_files;

    // Create threads for processing jobs
    for (int i = 0; i < MAX_THREADS && i < num_files; i++) {  
        pthread_create(&threads[i], NULL, process_jobs_thread, (void *)file_list);
        job_data = job_data->next;
    }

    // Wait for all threads to complete
    for (int i = 0; i < MAX_THREADS && i < num_files; i++) {  
        pthread_join(threads[i], NULL); 
    }

    // Registra a função de limpeza do FIFO para ser chamada ao sair
    atexit(cleanup_fifo);

    // Wait for all child processes (backups) to finish
    while (running_backups > 0) {                                                   // Wait for all child processes (backups) to finish
        pid_t pid = waitpid(-1, NULL, 0);
        if (pid > 0) {
            __sync_fetch_and_sub(&running_backups, 1);                              // Atomically decrement running backups counter
        } else if (pid == -1 && errno == ECHILD) {
            break;                                                                  // No more child processes
        }
    }

    // Free memory allocated for the file list
    Job_data *current_job = file_list->job_data;  
    while (current_job != NULL) {
        Job_data *next_job = current_job->next;
        free(current_job->file_path);
        free(current_job);
        current_job = next_job;
    }

    /*// Monitor the registration FIFO for client connections //FIXX MEEE
    int terminate = 0;
    while (!terminate) {
        char client_request[PIPE_BUF];
        ssize_t bytes_read = read(registration_fifo_fd, client_request, sizeof(client_request) - 1);
        if (bytes_read > 0) {
            client_request[bytes_read] = '\0';  // Null-terminate the request
            if (strcmp(client_request, "EXIT") == 0) {
                fprintf(stdout, "Termination request received.\n");
                terminate = 1;
            } else {
                fprintf(stdout, "Received client connection request: %s\n", client_request);
                // Process the client request here
            }
        } else if (bytes_read == 0) {
            // No data available
            usleep(100000);  // Sleep briefly to avoid high CPU usage in the loop
        } else {
            perror("Error reading from FIFO");
        }
    }
*/
    free(file_list);
    close(registration_fifo_fd);  
    kvs_terminate();                                                                // Terminate the KVS system
    return 0;
}

void handle_sigchld(int signo) {                                                    // Handler for SIGCHLD signals to clean up finished child processes
    (void)signo;                                                                    // Suppress unused parameter warning
    while (waitpid(-1, NULL, WNOHANG) > 0) {
        __sync_fetch_and_sub(&running_backups, 1);                                  // Atomically decrement running backups counter
    }
}


void perform_backup(const char *filename, int backup_num) {                         // Perform a backup operation in a child process
    char backup_filename[MAX_JOB_FILE_NAME_SIZE];                                   // Generate the backup file name
    snprintf(backup_filename, sizeof(backup_filename), "%.*s-%d.bck",
        (int)(strlen(filename) - 4), filename, backup_num+1);

    int backup_fd = open(backup_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);      // Open the backup file
    if (backup_fd == -1) {
        perror("Failed to create backup file");
        return;
    } else {
        fprintf(stderr, "Backup file created: %s\n", backup_filename);
    }

    if (kvs_backup(backup_fd) != 0) {                                               // Execute the backup operation
        fprintf(stderr, "Failed to write backup to %s\n", backup_filename);
        close(backup_fd);
        return;
    }

    close(backup_fd);
}

int process_job_file(const char *filename) {                                        // Process a .job file and execute the associated commands
    int backup_count = 0;                                                           // Counter for backups performed for this job

    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open file");
        return -1;
    }

    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    char output_filename[MAX_JOB_FILE_NAME_SIZE];                                   // Create the output file name
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
                num_pairs = (size_t)parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                kvs_delete(num_pairs, keys, output_fd);
                break;

            case CMD_SHOW:
                kvs_show(output_fd);
                break;

            case CMD_WAIT:
                if (parse_wait(fd, &delay, NULL) == -1) {
                    continue;
                }
                if (delay > 0) {
                    kvs_wait(delay); 
                }
                break;

            case CMD_BACKUP:
                kvs_wait_backup(filename, &backup_count);
                break;

            case CMD_INVALID:
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

void handle_client_session(int client_fifo_fd) {
    char client_command[PIPE_BUF];
    ssize_t cmd_bytes;

    while ((cmd_bytes = read(client_fifo_fd, client_command, sizeof(client_command) - 1)) > 0) {
        client_command[cmd_bytes] = '\0';

        if (strcmp(client_command, "DISCONNECT") == 0) {
            fprintf(stdout, "Client requested disconnect.\n");
            break;
        }

        fprintf(stdout, "Processing client command: %s\n", client_command);

        // Exemplo de resposta ao cliente
        dprintf(client_fifo_fd, "Server processed: %s\n", client_command);
    }

    fprintf(stdout, "Client session ended.\n");
}

File_list *process_directory(const char *dirpath) {                                 // Process all .job files in a given directory and create a job list
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

void *process_jobs_thread(void *arg) {                                                   // Process the job list using threads, assigning a thread to each job
    File_list *file_list = (File_list *)arg;
    Job_data *job_data = file_list->job_data;
    for (; job_data != NULL; job_data = job_data->next) {
        pthread_mutex_lock(&file_list->mutex);
        if (job_data->status == 0) {                                                // Process the job file if it has not been processed yet
            job_data->status = 1;
            process_job_file(job_data->file_path);
            pthread_mutex_unlock(&file_list->mutex);
            
        } else {
            pthread_mutex_unlock(&file_list->mutex);                                // Skip the job file if it has already been processed
        }
    }
    return NULL;
}
