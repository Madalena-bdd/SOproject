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
  char *file_path;                                              // Path to the .job file
  int output_fd;                                                // File descriptor for the output
  int running_backups;                                          // Number of backups currently running for this job
  int concurrent_backups;                                       // Maximum number of concurrent backups allowed
  int status;                                                   // 0 - Not processed, 1 - Processed or being processed
  struct Job_data *next;                                        // Pointer to the next job
} Job_data;

typedef struct {
  Job_data* job_data;                                           // Pointer to the list of jobs
  int num_files;                                                // Number of files in the directory
  pthread_mutex_t mutex;                                        // Mutex for thread synchronization
} File_list;

int MAX_THREADS = 0;                                            // Maximum number of threads, received as argument
int concurrent_backups = 0;                                     // Maximum number of concurrent backups, received as argument
int running_backups = 0;                                        // Number of backups currently running globally

void *process_jobs_thread(void *arg);
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

    if (kvs_init()) {                                             // Initializes the KVS system
        perror("Failed to initialize KVS");
        return 1;
    }

    signal(SIGCHLD, handle_sigchld);                              // Set up SIGCHLD handler for handling child process termination

    File_list *file_list = process_directory(dirpath);            // Process the directory to build the job list

    if (file_list == NULL) {
        return 1;
    }

    Job_data *job_data = file_list->job_data;
    pthread_t threads[MAX_THREADS];
    int num_files = file_list->num_files;

    for (int i = 0; i < MAX_THREADS && i < num_files; i++) {      // Create threads for processing jobs
        pthread_create(&threads[i], NULL, process_jobs_thread, (void *)file_list);
        job_data = job_data->next;
    }

    for (int i = 0; i < MAX_THREADS && i < num_files; i++) {      // Wait for all threads to complete
        pthread_join(threads[i], NULL);
    }
    
    while (running_backups > 0) {                                 // Wait for all child processes (backups) to finish     
        pid_t pid = waitpid(-1, NULL, 0);
        if (pid > 0) {
            __sync_fetch_and_sub(&running_backups, 1);            // Atomically decrement running backups counter 
        } else if (pid == -1 && errno == ECHILD) {
            break;                                                // No more child processes
        }
    }

    Job_data *current_job = file_list->job_data;                  // Free memory allocated for the job list
    while (current_job != NULL) {
        Job_data *next_job = current_job->next;
        free(current_job->file_path);
        free(current_job);
        current_job = next_job;
    }
    pthread_mutex_destroy(&file_list->mutex);
    free(file_list);
    kvs_terminate();                                              // Terminate the KVS system
    return 0;
}

void handle_sigchld(int signo) {                                  // Handler for SIGCHLD signals to clean up finished child processes
    (void)signo;                                                  // Suppress unused parameter warning
    while (waitpid(-1, NULL, WNOHANG) > 0) {                      // Atomically decrement running backups counter
        __sync_fetch_and_sub(&running_backups, 1); 
    }
}

void perform_backup(const char *filename, int backup_num) {       // Perform a backup operation in a child process
    char backup_filename[MAX_JOB_FILE_NAME_SIZE];                 // Generate the backup file name
    snprintf(backup_filename, sizeof(backup_filename), "%.*s-%d.bck",
        (int)(strlen(filename) - 4), filename, backup_num+1);

    int backup_fd = open(backup_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);        // Open the backup file
    if (backup_fd == -1) {
        perror("Failed to create backup file");
        return;
    } else {
        fprintf(stderr, "Backup file created: %s\n", backup_filename);
    }

    if (kvs_backup(backup_fd) != 0) {                                                 // Execute the backup operation
        fprintf(stderr, "Failed to write backup to %s\n", backup_filename);
        close(backup_fd);
        return;
    }

    close(backup_fd);
}

int process_job_file(const char *filename) {                       // Process a .job file and execute the associated commands
    int backup_count = 0;                                          // Counter for backups performed for this job

    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open file");
        return -1;
    }

    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    char output_filename[MAX_JOB_FILE_NAME_SIZE];                   // Create the output file name
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

File_list *process_directory(const char *dirpath) {                // Process all .job files in a given directory and create a job list
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
    file_list->job_data = NULL;
    file_list->mutex = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;

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

void *process_jobs_thread(void *arg) {                              // Process the job list using threads, assigning a thread to each job
    File_list *file_list = (File_list *)arg;
    Job_data *job_data = file_list->job_data;
    for (; job_data != NULL; job_data = job_data->next) {
        pthread_mutex_lock(&file_list->mutex);
        if (job_data->status == 0) {                                // Process the job file if it has not been processed yet
            job_data->status = 1;
            pthread_mutex_unlock(&file_list->mutex);
            process_job_file(job_data->file_path);
            
        } else {
            pthread_mutex_unlock(&file_list->mutex);                // Skip the job file if it has already been processed
        }
    }
    return NULL;
}
