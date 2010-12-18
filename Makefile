CFLAGS+=	-std=gnu99 -Wall -g
LDFLAGS+=	-lpthread

all: db

clean:
	-rm -f db
