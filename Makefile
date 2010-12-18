CFLAGS+=	-std=gnu99 -Wall -g -O0
LDFLAGS+=	-lpthread

all: db

clean:
	-rm -f db
