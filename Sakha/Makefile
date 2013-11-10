CC=gcc
CFLAGS=-Wall -std=c99
LDFLAGS=
SOURCES=main.c logger.c os_posix.c
EXECUTABLE=sakhadb
OBJECTS=$(SOURCES:.c=.o)

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJECTS) $(EXECUTABLE)

