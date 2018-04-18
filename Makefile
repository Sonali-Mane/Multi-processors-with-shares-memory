all: 
	gcc -pthread -o map mapper_sem.c -I.
	./map 5 7 <input.txt

