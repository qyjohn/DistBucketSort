CC=g++
CFLAGS=-std=c++11

all:
	g++ -std=c++11 SortPhase1.cpp -lpthread -o bin/SortPhase1
	g++ -std=c++11 SortPhase2.cpp -lpthread -o bin/SortPhase2

