#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>

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
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
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
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
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

  return 0;
}


void kvs_show(int output_fd) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      dprintf(output_fd, "(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next;
    }
  }
}


int kvs_backup(int output_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
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
      // Incrementar o contador de backups para o arquivo atual
    (*backup_count)++;

    // Aguardar até que o número de backups em execução esteja abaixo do limite
    while (__sync_fetch_and_add(&running_backups, 0) >= concurrent_backups) {
        waitpid(-1, NULL, 0); // Espera que algum processo filho termine
    }

    // Criar um novo processo filho para realizar o backup
    pid_t pid = fork();
    if (pid == 0) {
        // Processo filho
        perform_backup(filename, *backup_count);
    } else if (pid > 0) {
        // Processo pai: incrementa atomicamente o contador de backups em execução
        __sync_fetch_and_add(&running_backups, 1);
    } else {
        perror("Failed to fork process for backup");
    }
}


void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}