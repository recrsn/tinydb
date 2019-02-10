OBJ = database.o

%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS)

database: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

clean:
	-rm database *.o *~