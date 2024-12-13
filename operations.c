#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <pthread.h>

#include "kvs.h"
#include "constants.h"
#include "operations.h"

static struct HashTable* kvs_table = NULL;
extern int concurrent_backups;
extern int running_backups;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

// Initializes the key-value store (KVS)
int kvs_init() {                                                 
    if (kvs_table != NULL) {    
        char error_message[MAX_STRING_SIZE];
        snprintf(error_message, MAX_STRING_SIZE, "KVS state has already been initialized\n");
        write(STDERR_FILENO, error_message, strlen(error_message));
        return 1;
    }
    kvs_table = create_hash_table();
    return kvs_table == NULL;
}

// Terminates the key-value store (KVS)
int kvs_terminate() {                                            
    if (kvs_table == NULL) {
        char error_message[MAX_STRING_SIZE];
        snprintf(error_message, MAX_STRING_SIZE, "KVS state must be initialized\n");
        write(STDERR_FILENO, error_message, strlen(error_message));
        return 1;
    }
    free_table(kvs_table);
    return 0;
}

void mutex_hash_lock(char keys[][MAX_STRING_SIZE], size_t num_pairs) {
    int* hash_values = (int*)malloc(num_pairs * sizeof(int));
    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(keys[i]);
        hash_values[i] = index;
    }
    qsort(hash_values, num_pairs, sizeof(int), (int (*)(const void*, const void*)) strcmp);
    size_t unique_count = 0;
    for (size_t i = 1; i < num_pairs; i++) {
        if (hash_values[i] != hash_values[unique_count]) {
            unique_count++;
            hash_values[unique_count] = hash_values[i];
        }
    }
    
    hash_values = (int*)realloc(hash_values, (unique_count + 1) * sizeof(int));

    for (size_t i = 0; i <= unique_count; i++) {
        pthread_mutex_lock(&kvs_table->list_mutex[hash_values[i]]);
    }
    free(hash_values);
}

void mutex_hash_unlock(char keys[][MAX_STRING_SIZE], size_t num_pairs) {
    int* hash_values = (int*)malloc(num_pairs * sizeof(int));
    for (size_t i = 0; i < num_pairs; i++) {
        int index = hash(keys[i]);
        hash_values[i] = index;
    }
    qsort(hash_values, num_pairs, sizeof(int), (int (*)(const void*, const void*)) strcmp);
    size_t unique_count = 0;
    for (size_t i = 1; i < num_pairs; i++) {
        if (hash_values[i] != hash_values[unique_count]) {
            unique_count++;
            hash_values[unique_count] = hash_values[i];
        }
    }
    
    hash_values = (int*)realloc(hash_values, unique_count * sizeof(int));

    for (size_t i = 0; i < unique_count; i++) {
        pthread_mutex_unlock(&kvs_table->list_mutex[hash_values[i]]);
    }
    free(hash_values);
}

// Writes one or more key-value pairs to the KVS
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) { 
    mutex_hash_lock(keys, num_pairs);
    if (kvs_table == NULL) {
        char error_message[MAX_STRING_SIZE];
        snprintf(error_message, MAX_STRING_SIZE, " write KVS state must be initialized\n");
        write(STDERR_FILENO, error_message, strlen(error_message));
        return 1;
    }
    for (size_t i = 0; i < num_pairs; i++) {
        if (write_pair(kvs_table, keys[i], values[i]) != 0) {
            char error_message[MAX_STRING_SIZE];
            snprintf(error_message, MAX_STRING_SIZE, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
            write(STDERR_FILENO, error_message, strlen(error_message));
        }
    }
    mutex_hash_unlock(keys, num_pairs);
    return 0;
}

// Reads one or more key-value pairs from the KVS
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {   
    mutex_hash_lock(keys, num_pairs);              
    if (kvs_table == NULL) {
        char error_message[MAX_STRING_SIZE];
        snprintf(error_message, MAX_STRING_SIZE, " read KVS state must be initialized\n");
        write(STDERR_FILENO, error_message, strlen(error_message));
        return 1;
    }

    dprintf(output_fd, "[");
    for (size_t i = 0; i < num_pairs; i++) {
        char* result = read_pair(kvs_table, keys[i]);
        if (result == NULL) {
            dprintf(output_fd,"(%s,KVSERROR)", keys[i]);                                      // When the key is not found
        } else {
            dprintf(output_fd,"(%s,%s)", keys[i], result);                                    // When the key is found
        }
        free(result);
    }
    dprintf(output_fd, "]\n");
    mutex_hash_unlock(keys, num_pairs);
    return 0;
}

// Deletes one or more key-value pairs from the KVS
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {             
    mutex_hash_lock(keys, num_pairs);

    if (kvs_table == NULL) {
        char error_message[MAX_STRING_SIZE];
        snprintf(error_message, MAX_STRING_SIZE, "delete KVS state must be initialized\n");
        write(STDERR_FILENO, error_message, strlen(error_message));
        return 1;
    }
    int aux = 0;

    for (size_t i = 0; i < num_pairs; i++) {
        if (delete_pair(kvs_table, keys[i]) != 0) {
            if (!aux) {
                dprintf(output_fd,"[");
                aux = 1;
            }
            dprintf(output_fd,"(%s,KVSMISSING)", keys[i]);                                   // When the key is not found
        }
    }
    if (aux) {
        dprintf(output_fd,"]\n");
    }
    mutex_hash_unlock(keys, num_pairs);
    return 0;
}


// Writes the state of the KVS
void kvs_show(int output_fd) {                                                                  
    for (int i = 0; i < TABLE_SIZE; i++) {
        pthread_mutex_lock(&kvs_table->list_mutex[i]);
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL) {
            dprintf(output_fd, "(%s, %s)\n", keyNode->key, keyNode->value);
            keyNode = keyNode->next;
        }
        pthread_mutex_unlock(&kvs_table->list_mutex[i]);
    }
}

// Creates a backup of the KVS state
int kvs_backup(int output_fd) {                                                             
    if (kvs_table == NULL) {
        char error_message[MAX_STRING_SIZE];
        snprintf(error_message, MAX_STRING_SIZE, "backup KVS state must be initialized\n");
        write(STDERR_FILENO, error_message, strlen(error_message));
        return 1;
    }

    for (int i = 0; i < TABLE_SIZE; i++) {                                                   // Iterate over the elements of the table and writes them
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL) {
            dprintf(output_fd, "(%s, %s)\n", keyNode->key, keyNode->value);
            keyNode = keyNode->next;
        }
    }
    return 0;                                                                                // Backup was successful   
}

// Waits for the last backup to be called
void kvs_wait_backup(const char *filename, int *backup_count) {                          
    while(1){
        if (running_backups <= concurrent_backups) {
            break;
        }
        wait(NULL);
    }
    pid_t pid = fork();

    if (pid == 0) {
        perform_backup(filename, *backup_count);                                             // Child process 
        _exit(EXIT_SUCCESS);
    } else if (pid > 0) {
        (*backup_count)++;                                                                   // Parent process
        __sync_fetch_and_add(&running_backups, 1);                                           // Atomically increment the counter

        pid_t child_pid = waitpid(pid, NULL, 0);                                             // Wait for the child process to finish
        if (child_pid > 0) {
            __sync_fetch_and_sub(&running_backups, 1);                                       // Atomically decrement after success
        }
    } else {
    perror("Failed to fork process for backup");                                             // Error handling    
    }
}

void kvs_wait(unsigned int delay_ms) {                                                       // Waits for a given amount of time
    struct timespec delay = delay_to_timespec(delay_ms); 
    nanosleep(&delay, NULL);
}
