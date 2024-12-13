#include <ctype.h>
#include <pthread.h>
#include <stdlib.h>
#include "kvs.h"
#include "string.h"

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project
int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1;                                                  // Invalid index for non-alphabetic or number strings
}

struct HashTable* create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
      ht->table[i] = NULL;
      pthread_mutex_init(&ht->list_mutex[i], NULL);
  }
  pthread_mutex_init(&ht->table_mutex, NULL);
  return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    pthread_mutex_lock(&ht->list_mutex[index]);

    while (keyNode != NULL) {                                   // Search for the key node
        if (strcmp(keyNode->key, key) == 0) {   
            free(keyNode->value);
            keyNode->value = strdup(value);
            pthread_mutex_unlock(&ht->list_mutex[index]);
            return 0;
        }
        keyNode = keyNode->next;                               // Move to the next node
    }

    keyNode = malloc(sizeof(KeyNode));                         // Key not found, create a new key node
    keyNode->key = strdup(key);                                // Allocate memory for the key
    keyNode->value = strdup(value);                            // Allocate memory for the value
    keyNode->next = ht->table[index];                          // Link to existing nodes
    ht->table[index] = keyNode;                                // Place new key node at the start of the list
    pthread_mutex_unlock(&ht->list_mutex[index]);
    return 0;
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    pthread_mutex_lock(&ht->list_mutex[index]);
    char* value = NULL;                                        // Initialize value to NULL

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            break;                                             // Exit the loop once the key is found
        }
        keyNode = keyNode->next;                               // Move to the next node
    }
    pthread_mutex_unlock(&ht->list_mutex[index]);
    return value;                                              // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;

    while (keyNode != NULL) {                                 // Search for the key node
        if (strcmp(keyNode->key, key) == 0) {
            if (prevNode == NULL) {                           // Key found; delete this node
                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next;             // Update the table to point to the next node
            } else {
                // Node to delete is not the first; bypass it
                prevNode->next = keyNode->next;               // Link the previous node to the next node
            }
            // Free the memory allocated for the key and value
            free(keyNode->key);
            free(keyNode->value);
            free(keyNode);                                    // Free the key node itself
            return 0;                                         // Exit the function
        }
        prevNode = keyNode;                                   // Move prevNode to current node
        keyNode = keyNode->next;                              // Move to the next node
    }
    
    return 1;
}

void free_table(HashTable *ht) {
    pthread_mutex_lock(&ht->table_mutex);
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->key);
            free(temp->value);
            free(temp);
        }
        pthread_mutex_destroy(&ht->list_mutex[i]);
    }
    pthread_mutex_unlock(&ht->table_mutex);
    pthread_mutex_destroy(&ht->table_mutex);
    free(ht);
}
