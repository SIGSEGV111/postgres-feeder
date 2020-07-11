.PHONY: all clean test

all: postgres-feeder

postgres-feeder: postgres-feeder.cpp Makefile
	g++ -fwhole-program -Wall -Wextra -flto -O0 -g -march=native -fdata-sections -ffunction-sections -Wl,--gc-sections postgres-feeder.cpp -l pq -o postgres-feeder -Wno-unused-variable
# 	strip postgres-feeder

test: postgres-feeder
	valgrind --quiet --trace-children=yes --track-fds=yes --leak-check=yes ./postgres-feeder 'bsf' 'dbname=spihome'

clean:
	rm -vf postgres-feeder
