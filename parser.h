#ifndef KVS_PARSER_H
#define KVS_PARSER_H

#include <stddef.h>
#include "constants.h"

enum Command {
    CMD_WRITE,
    CMD_READ,
    CMD_DELETE,
    CMD_SHOW,
    CMD_WAIT,
    CMD_BACKUP,
    CMD_HELP,
    CMD_EMPTY,
    CMD_INVALID,
    EOC  // End of commands
};

/// Reads a command from a line and returns the corresponding command type.
/// @param line The line to parse.
/// @return The command read.
enum Command get_next(int fd);

/// Parses a WRITE command from a line.
/// @param line The line containing the WRITE command.
/// @param keys Array to store parsed keys.
/// @param values Array to store parsed values.
/// @param max_keys Maximum number of key-value pairs to parse.
/// @param max_string_size Maximum size for keys and values.
/// @return The number of key-value pairs parsed, or 0 on error.
size_t parse_write(int fd, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t max_pairs, size_t max_string_size);

/// Parses a READ or DELETE command from a line.
/// @param line The line containing the READ or DELETE command.
/// @param keys Array to store parsed keys.
/// @param max_keys Maximum number of keys to parse.
/// @param max_string_size Maximum size for keys.
/// @return The number of keys parsed, or 0 on error.
size_t parse_read_delete(int fd, char keys[][MAX_STRING_SIZE], size_t max_keys, size_t max_string_size);

/// Parses a WAIT command from a line.
/// @param line The line containing the WAIT command.
/// @param delay Pointer to store the parsed delay (in milliseconds).
/// @param thread_id Pointer to store the thread ID, if specified (optional).
/// @return 0 if the delay was parsed successfully, or -1 on error.
int parse_wait(int fd, unsigned int *delay, unsigned int *thread_id);

#endif  // KVS_PARSER_H
