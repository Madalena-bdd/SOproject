CC = gcc

# Flags de compilação
CFLAGS = -g -std=c17 -D_POSIX_C_SOURCE=200809L \
		 -Wall -Werror -Wextra \
		 -Wcast-align -Wconversion -Wfloat-equal -Wformat=2 -Wnull-dereference -Wshadow -Wsign-conversion -Wswitch-enum -Wundef -Wunreachable-code -Wunused -pthread

ifneq ($(shell uname -s),Darwin) # if not MacOS
	CFLAGS += -fmax-errors=5
endif

# Alvo principal
all: kvs

# Regra para o executável principal
kvs: main.c constants.h operations.o parser.o kvs.o
	@$(CC) $(CFLAGS) -o kvs main.c operations.o parser.o kvs.o -lpthread

# Regra genérica para arquivos .o (com header correspondente)
%.o: %.c %.h
	@$(CC) $(CFLAGS) -c $<

# Limpeza de arquivos gerados
clean:
	@rm -f *.o kvs
	@rm -rf *.dSYM

# Execução do programa
run: kvs
	@./kvs

# Formatação do código
format:
	@which clang-format >/dev/null 2>&1 || echo "Please install clang-format to run this command"
	clang-format -i *.c *.h

stress_test: kvs
	@for i in {1..50}; do \
		./kvs jobs 4 5; \
		echo "Done $$i."; \
	done
