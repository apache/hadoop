AR	=ar
CC	=gcc
CFLAGS	=-ansi -Wall -O2 -fPIC

OBJS	=uriparser2.o uriparser/UriParse.o uriparser/UriParseBase.o uriparser/UriCommon.o uriparser/UriIp4Base.o uriparser/UriIp4.o

all:	lib staticlib

test:	staticlib
	$(CC) -o test-uriparser2 test-uriparser2.c liburiparser2.a
	./test-uriparser2

lib:	$(OBJS)
	$(CC) -shared -o liburiparser2.so $(OBJS)

staticlib:	$(OBJS)
	$(AR) cr liburiparser2.a $(OBJS)

clean:
	rm -rf liburiparser2.so liburiparser2.a test-uriparser2 $(OBJS)
