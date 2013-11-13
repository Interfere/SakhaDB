CC=gcc
CFLAGS=-Wall -std=c99 -DDEBUG=1 -O0 -Wno-trigraphs -Wno-missing-field-initializers -Wno-missing-prototypes -Werror=return-type -Wno-missing-braces -Wparentheses -Wswitch -Wunused-function -Wno-unused-label -Wno-unused-parameter -Wunused-variable -Wunused-value -Wempty-body -Wuninitialized -Wno-unknown-pragmas -Wno-shadow -Wno-four-char-constants -Wno-conversion -Wpointer-sign -Wno-newline-eof
LDFLAGS=
SOURCES=main.c logger.c os_posix.c sakhadb.c
EXECUTABLE=sakhadb
OBJECTS=$(SOURCES:.c=.o)

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJECTS) $(EXECUTABLE)

