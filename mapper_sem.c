#include<stdio.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>// sem library
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h> /* Declares errno and defines error constants */
#define MAXCHAR 10000

struct Tuples {
  
  char userID[5];
  char topic[16];
  int score;
};

struct Tuples buffer[MAXCHAR]={0};
struct Tuples **buffer1;

sem_t *full_sem;  /* when 0, buffer is full */
sem_t *empty_sem;
sem_t *buf_lock;

int *count, *in, *out,*done;
int *size_of_buff,*num_red_threads;
char* filename;

void consumer(int param);
void producer();

int main(int argc, char *argv[]) 
{
	
	int i=0;
	
	count = mmap(NULL, sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	*count=0;	
	
	size_of_buff= mmap(NULL, sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

	num_red_threads= mmap(NULL, sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	*num_red_threads= atoi(argv[2]);

	*size_of_buff=atoi(argv[1]);
	
	buffer1= (struct Tuples **)mmap(NULL, sizeof(struct Tuples *) * (*num_red_threads), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	*buffer1 = (struct Tuples *)mmap(NULL, sizeof(struct Tuples) * (*num_red_threads) * *(size_of_buff), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

	in= (int *)mmap(NULL, sizeof(int)*(*num_red_threads), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	done= (int *)mmap(NULL, sizeof(int)*100, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);	
	out= (int *)mmap(NULL, sizeof(int)*(*num_red_threads), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);		

	
	full_sem = (sem_t *)mmap(NULL, (*num_red_threads) * sizeof(sem_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);	
	empty_sem = (sem_t *)mmap(NULL, (*num_red_threads) * sizeof(sem_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);	
	buf_lock = (sem_t *)mmap(NULL, (*num_red_threads) * sizeof(sem_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);	

    for(i = 0; i < *num_red_threads; i++)
        buffer1[i] = (*buffer1 + (*size_of_buff) * i);

   for(i=0; i< *num_red_threads; i++)
	{
		in[i]=0;
		out[i]=0;
	}

	pthread_t producer_thread[1];
	pthread_t consumer_threads[*num_red_threads];



	//initialize semaphores
	for(i=0;i< *num_red_threads;i++)
	{
		sem_init(&buf_lock[i],1,1);
		sem_init(&full_sem[i],1,0);
		sem_init(&empty_sem[i],1,0);
	}
	
	switch (fork()) 
	{           /* Parent and child share mapping */
		case -1:

        case 0: /* Child: increment shared integer and exit */
  		 producer();

     }

 
	for(i=0;i< *num_red_threads;i++)
	{
		 switch (fork()) 
		 {           /* Parent and child share mapping */
				case -1:

				case 0: /* Child: increment shared integer and exit */
				consumer(i);
		} 
	}
	
	for(i=0;i< *num_red_threads;i++)
	{	
		wait(NULL);	
	}
	
	wait(NULL);

}


void consumer(int param)
{
	int tid;
	tid=param;
	const char s[4] = "()";
	const char newLine[4] = "\n";
	const char comma[2] = ",";
	char *token;
	struct Tuples inputTuple[10000]={0};
	int j = 0, i = 0, init = 0, k = 0, addVal = 0;
	char str[MAXCHAR];
	char *userId;
	char *topic;
	char *score;

	int index = 0,buff_index=0,buff_index_in=0,index_process=0;

	while(1)
	{	
		sem_wait(&buf_lock[tid]);

		while((in[tid]-out[tid])==0)// buffer empty
		{    	
			sem_post(&buf_lock[tid]);
			sem_wait(&empty_sem[tid]);
			sem_wait(&buf_lock[tid]);
		}

		printf("\n-------------------------------------------------------------------------------");	

		while ((in[tid]-out[tid])>0) 
		{ 
			userId = buffer1[tid][buff_index].userID;
			topic = buffer1[tid][buff_index].topic;
			score = buffer1[tid][buff_index].score;
			
			printf("\n\Data in Buffer %s:\t%s, %s, %d",userId,userId,topic,score);
			
			for(i=0;i<4;i++)
				inputTuple[buff_index_in].userID[i]=buffer1[tid][buff_index].userID[i];
			for(i=0;i<15;i++)
			{
				inputTuple[buff_index_in].topic[i]=buffer1[tid][buff_index].topic[i];
			}
			
			inputTuple[buff_index_in].score=buffer1[tid][buff_index].score;

			index++;
			buff_index++;
			buff_index_in++;  
			out[tid]++;
		}

		index_process=index;
		for(i=0;i<index_process;i++)
		{
			for(k=i+1;k<index_process;k++)
			{ 
				if((strcmp(inputTuple[i].userID, inputTuple[k].userID)==0)&& (strcmp(inputTuple[i].topic, inputTuple[k].topic)==0))
				{   
					addVal=inputTuple[i].score+inputTuple[k].score;
					inputTuple[i].score=addVal;

					for(j=k;j<index_process-1;j=j+1)
					{   
						inputTuple[j]=inputTuple[j+1];
					}

					index_process=index_process-1;
					buff_index_in--;	  
					k=k-1;
					index --;
				}    


			} 

		}
		
		printf("\n-------------------------------------------------------------------------------");	
		printf("\n");

		for (i = 0; i < index_process; i = i +1 ) 
		{

			printf("\nprocessed data for Buffer %s:\t(%s,%s,%d)\n", inputTuple[i].userID, inputTuple[i].userID, inputTuple[i].topic, inputTuple[i].score);
		}	

		out[tid]=0;
		buff_index=0;
		in[tid]=0;

		if(done[tid]==1)
		{
			sem_post(&buf_lock[tid]);
			break;
		}


		sem_post(&full_sem[tid]); 
		sem_post(&buf_lock[tid]);

	}
 
	exit(NULL);
}


void producer()
{


	const char s[4] = "()";
	const char comma[2] = ",";
	char *token;
	FILE *fp,*fw;
	int j=0,i,init=0,loop=0,counter=0, global_buff_size=*(size_of_buff),buff_size=0;   
	char *wordsToken[500];
	char *eachWord[500];
	char *userID[500],*topic[500],*action[500],*uniqueIds[400];
	char source[MAXCHAR];
	struct Tuples buffer[MAXCHAR]={0};     
	long buffer_size = 0;   
	
	fgets(source, MAXCHAR, stdin);
	token = strtok(source, s);
	
	while( token != NULL )
	{
		wordsToken[j++] = token;         
		token = strtok(NULL, s);
	}
	
	for( i = 0; i <j; i=i+2)
	{  
		init=0;
		token = strtok(wordsToken[i], comma);
		while( token != NULL )
		{
			eachWord[init++] = token;
			token = strtok(NULL, comma);
		} 
		
		userID[counter]=eachWord[0];
		topic[counter]=eachWord[2];
		action[counter]=eachWord[1];	
		counter++;
	}
	
	init=0;
	
	for(i=0;i<counter;i++)
	{
		for (j=0; j<i; j++)
		{
			if (strcmp(userID[j], userID[i])==0)
			break;	
		}
		
		if (i == j)
		{ 
			uniqueIds[init]=userID[i];
			init++;
		}
	}


	for(i=0;i< *num_red_threads;i++)
	{
		buff_size=0;
		for(j=0;j<counter;j++)
		{
			if(strcmp(uniqueIds[i], userID[j])==0)
			{ 
				sem_wait(&buf_lock[i]);
				if(in[i]==(*size_of_buff))
				{ 
					sem_post(&empty_sem[i]); 	     
					sem_post(&buf_lock[i]);
					sem_wait(&full_sem[i]);
					sem_wait(&buf_lock[i]);	
					in[i]=0;
					buff_size=0;
				}

				if(in[i] < global_buff_size)
				{
					in[i]=in[i]+1;	
					for(loop=0;loop<4;loop++)
					buffer1[i][buff_size].userID[loop]=userID[j][loop];	  
					for(loop=0;loop<15;loop++)
					buffer1[i][buff_size].topic[loop]=topic[j][loop];
					if(strcmp("P", action[j])==0)
					buffer1[i][buff_size].score=50;

					if(strcmp("L", action[j])==0)
					buffer1[i][buff_size].score=20;

					if(strcmp("D", action[j])==0)
					buffer1[i][buff_size].score=-10;	

					if(strcmp("C", action[j])==0)
					buffer1[i][buff_size].score=30;

					if(strcmp("S", action[j])==0)
					buffer1[i][buff_size].score=40;

					buff_size++; 
					sem_post(&buf_lock[i]); 
				}

			}
		}
		sem_post(&empty_sem[i]);

		done[i]=1;

	}
	exit(NULL);
}
