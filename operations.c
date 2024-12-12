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

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
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
    return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
    if (kvs_table == NULL) {
        char error_message[MAX_STRING_SIZE];
        snprintf(error_message, MAX_STRING_SIZE, " read KVS state must be initialized\n");
        write(STDERR_FILENO, error_message, strlen(error_message));
        return 1;
    }

    qsort(keys, num_pairs, sizeof(keys[0]), (int (*)(const void*, const void*)) strcmp);   // Ordenar as chaves alfabeticamente

    dprintf(output_fd, "[");
    for (size_t i = 0; i < num_pairs; i++) {
        char* result = read_pair(kvs_table, keys[i]);
        if (result == NULL) {
            dprintf(output_fd,"(%s,KVSERROR)", keys[i]);  // Quando a chave não é encontrada
        } else {
            dprintf(output_fd,"(%s,%s)", keys[i], result);  // Quando a chave é encontrada
        }
        free(result);
    }
    dprintf(output_fd, "]\n");
    return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  pthread_mutex_lock(&kvs_table->table_mutex);

    if (kvs_table == NULL) {
        char error_message[MAX_STRING_SIZE];
        snprintf(error_message, MAX_STRING_SIZE, "delete KVS state must be initialized\n");
        write(STDERR_FILENO, error_message, strlen(error_message));
        pthread_mutex_unlock(&kvs_table->table_mutex); 
        return 1;
    }
    int aux = 0;

    for (size_t i = 0; i < num_pairs; i++) {
        if (delete_pair(kvs_table, keys[i]) != 0) {
            if (!aux) {
                dprintf(output_fd,"[");
                aux = 1;
            }
            dprintf(output_fd,"(%s,KVSMISSING)", keys[i]);
        }
    }
    if (aux) {
        dprintf(output_fd,"]\n");
    }
    pthread_mutex_unlock(&kvs_table->table_mutex);
    return 0;
}


void kvs_show(int output_fd) {
    pthread_mutex_lock(&kvs_table->table_mutex);
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL) {
            dprintf(output_fd, "(%s, %s)\n", keyNode->key, keyNode->value);
            keyNode = keyNode->next;
        }
    }
    pthread_mutex_unlock(&kvs_table->table_mutex);
}


int kvs_backup(int output_fd) {
    if (kvs_table == NULL) {
        char error_message[MAX_STRING_SIZE];
        snprintf(error_message, MAX_STRING_SIZE, "backup KVS state must be initialized\n");
        write(STDERR_FILENO, error_message, strlen(error_message));
        return 1;
    }

    // Iterar sobre os elementos da tabela e escrevê-los no output_fd
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];
        while (keyNode != NULL) {
            dprintf(output_fd, "(%s, %s)\n", keyNode->key, keyNode->value);
            keyNode = keyNode->next;
        }
    }
    return 0; // Sucesso
}

void kvs_wait_backup(const char *filename, int *backup_count) { 
    while(1){
        if (running_backups <= concurrent_backups){
            break;
        }
        wait(NULL);
    }
    pid_t pid = fork();

    if (pid == 0) {
        // Processo filho
        perform_backup(filename, *backup_count);
        exit(EXIT_SUCCESS);
    } else if (pid > 0) {
        // Processo pai
        (*backup_count)++;
        __sync_fetch_and_add(&running_backups, 1);  // Incrementa atomicamente o contador

        // Espera pelo processo filho
        pid_t child_pid = waitpid(pid, NULL, 0);
        if (child_pid > 0) {
            __sync_fetch_and_sub(&running_backups, 1);  // Decrementa atomicamente após o sucesso
        }
    } else {
    // fork falhou
    perror("Failed to fork process for backup");
    }
}

void kvs_wait(unsigned int delay_ms) {
    struct timespec delay = delay_to_timespec(delay_ms);
    nanosleep(&delay, NULL);
}
