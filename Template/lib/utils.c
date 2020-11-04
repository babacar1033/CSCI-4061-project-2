#include "utils.h"
#include "mapper.h"//to use mapperID variable
#include <stdbool.h>//to use boolean variable
#include <sys/msg.h>//-->in order to use message queues
#define PERM 0666//--> user, group, and others each have only read and write permissions
#define ERROR 0//for errors in the code
#define LESS_THAN_CHUNKSIZE 1
#define EQUAL_CHUNKSIZE 2
#define MORE_THAN_CHUNKSIZE 3

char *getChunkData(int mapperID)

{



  //declare the same key as in the "sendchunkdata". To be used again to open the message queue.

  key_t key = ftok(".",5331326); // convert the pathname "key" and the reducer identifier to a System V IPC

  //declare a variable msg of type struct msgBuffer to represent the chunk4

  struct msgBuffer msg;

  //open mesage queue

  int mid = msgget(key, PERM| IPC_CREAT); // use permission 0644 where user, groups and other can read and write. create the message queue if it's not done so yet

  if(mid == -1)
  {
    exit(ERROR); //error handling. return -1 and set errno
  }
  
  //struct msgBuffer *ptr = malloc(MSGSIZE);

  int checkStatus = msgrcv(mid, &msg, sizeof(msg.msgText), mapperID, 0); //receive data from the master who was supposed to send a specific mapperID
  if(checkStatus == -1){
  	perror("Could not receive the data");
  	exit(ERROR);
  }else{
  	//free(ptr);
  	char *v = msg.msgText;
  	return v;
  }
  
}

//helper function for sendChunkData()
int compareToChunkSize(int currentChunkSize, int characterCount, char *wholeString, int nMappers, struct msgBuffer msg){
    //int ok;
    //int msgid
    
    key_t key;
    int msgid, ok;
    
    //generate unique key
    key = ftok(".", 5331326);//--> use your x500 as the second argument of ftok(): a TA talks about this in the most recent Canvas announcement

    //creates a message queue
    msgid = msgget(key, PERM | IPC_CREAT);
    
    if(currentChunkSize < chunkSize){
        strcat(msg.msgText,wholeString);//--> concatenate wholeString to msg.text (which is chunk data thus far)
        strcat(msg.msgText," ");//--> whitespace character
        memset(wholeString, '\0', chunkSize);//--> so reset "wholeString"
        msgctl(msgid, IPC_RMID, NULL);//close the message queue before I leave this function
        return LESS_THAN_CHUNKSIZE;
    }else if(currentChunkSize > chunkSize){
    	bool yes = true;
    	int i;
    	for(i = 0; i < MSGSIZE; i++){
    		if(msg.msgText[i] != '\0'){
    			yes = false;//I did not call memset on msg.msgText
    		}
    	}
        if(true){//msg.text is just null terminators
            perror("Word in chunk is too long - cannot be sent without being separated.");
            exit(ERROR);
        }
        //--> then DO NOT concatenate whole string to "msg.text"
        ok = msgsnd(msgid, (void *) &msg, MSGSIZE, mapperID);
        if(ok == -1){
            perror("Could not send the data");
            exit(ERROR);
        }
        
        
        if(mapperID < nMappers){
            mapperID++;
        }
        msgctl(msgid, IPC_RMID, NULL);//close the message queue before I leave this function
        return MORE_THAN_CHUNKSIZE;
    }else{
        strcat(msg.msgText,wholeString);//--> concatenate wholeString to msg.text (which is chunk data thus far)
        strcat(msg.msgText," ");//--> whitespace character
        ok = msgsnd(msgid, (void *) &msg, MSGSIZE, mapperID);
        if(ok == -1){
            perror("Could not send the data");
            exit(ERROR);
        }
        if(mapperID < nMappers){
            mapperID++;
        }
        //--> reset characterCount, currentChunkSize, and "msg.text" (chunk data)
        characterCount = 0;
        currentChunkSize = 0;
        memset(msg.msgText, '\0', MSGSIZE);//--> reset "msg.text" (chunk data)
        memset(wholeString, '\0', chunkSize);//--> so reset "wholeString"
        msgctl(msgid, IPC_RMID, NULL);//close the message queue before I leave this function
        return EQUAL_CHUNKSIZE;
    }
}





// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers)

{
	key_t key;
	int msgid, ok, ok2;
	struct msgBuffer msg;
	char checkString;//--> I declared this here because I do not believe you declared it somewhere
	int characterCount = 0;//--> use this to see if a word is shared between the end of one chunk and the beginning of the next chunk
    	char wholeString[chunkSize];//--> temporary storage
    	memset(wholeString, '\0', chunkSize);//--> to make sure it is always null-terminated
    	int currentChunkSize = 0;

	//generate unique key
	key = ftok(".", 5331326);//--> use your x500 as the second argument of ftok(): a TA talks about this in the most recent Canvas announcement

	//creates a message queue
	msgid = msgget(key, PERM | IPC_CREAT);

	FILE* file = fopen (inputFile, "r");
	//char line[chunkSize];//--> chunkSize Macro is defined in "utils.h"

	//--> error checking for fopen() --> see if the file pointer points to anything non-NULL
	if(file == NULL){
        	perror("This file could not be opened");
        	exit(ERROR);
    	}

    	mapperID = 1;

	//construct chunks of 1024 bytes each and send
	//each chunk to a mapper
	checkString = fgetc (file);
	while (checkString != EOF){//--> check to see if the pointer is not at the end-of-file
		characterCount++;//--> a character has been read for current chunk
		if(checkString != ' '){
            		strncat(wholeString,&checkString, 1);//--> concatenate to wholeString - size of one character is 1 byte
		}else{//--> checkString != ' '
            //--> if at this point, we read one whole string in inputFile
            //--> add size of a whole string
            currentChunkSize += (characterCount-1);//--> the "-1" is to account for the space
            int x = compareToChunkSize(currentChunkSize, characterCount, wholeString, nMappers, msg);
            if(x == MORE_THAN_CHUNKSIZE){
                memset(msg.msgText, '\0', MSGSIZE);//--> reset "line" (chunk)
                compareToChunkSize((characterCount-1), characterCount, wholeString, nMappers, msg);
            }
		}
	    checkString = fgetc (file);//update file pointer
	    
	}

	//send end message to mappers
	for (int i=0; i<nMappers; i++){
		msg.msgType = i;//--> use mapperID (i) as the tag
		memset(msg.msgText, '\0', MSGSIZE);
		sprintf(msg.msgText, "END");
		ok2 = msgsnd(msgid, (void *)&msg, MSGSIZE, i);
	}
	/*

    //--> is this for the ACK??
	for (int i =0; i<nMappers; i++){
		wait(msgid);
	}
	*/

	//close message queue
	msgctl(msgid, IPC_RMID, NULL);

	fclose(file);
}


// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
    struct msgBuffer one;
    int messageID;
    key_t key1;

    key1 = ftok(".", 5331326);

    //open message queue
    messageID = msgget (key1, PERM | IPC_CREAT);

    key1 = msgrcv (messageID, (void *)&one, sizeof (one.msgText), reducerID, 0);
    //TODO: Error checking for

    //check for END message
    if(one.msgText == "END"){
        return 0;//done reading data
    }else{
        strcpy(key, one.msgText);//copy m.msgText into key
        return 1;//more data to come
    }
}

void shuffle(int nMappers, int nReducers)

{
  char buff[1024]; // a buffer to store the word filepath

  //declare the same key as in the "sendchunkdata". To be used again to open the message queue.

  key_t key = ftok("project", 2); // convert the pathname "key" and the reducer identifier to a System V IPC

  //declare a variable msg of type struct msgBuffer to represent the chunk

  struct msgBuffer msg;

  //open mesage queue

  int mid = msgget(key, PERM | IPC_CREAT); // use permission 0644 where user, groups and other can read and write. create the message queue if it's not done so yet

  // preparation of traversing the directory of each Mapper and send the word filepath to the reducers

  struct dirent *entry;


  for (int i = 0; i < nMappers; i += 1)
  {
    sprintf(buff, "output/MapOut/Map_%d", i+1); //copy word filepath to buffer

    DIR *dir = opendir(buff);  // open mapOutdirectory


    //check if directory exists.

    if (dir == NULL)
    {
      //perror("The path passed is invalid");
      //exit(ERROR);
    }


    while (entry = readdir(dir)) // traverse the directory of 1 mapper
    {
      if (entry->d_type == DT_REG) //verify if the type entry is pointing to is a file. if so select the reducer using a hash function

      {
        int reducerId = 1 + hashFunction(entry->d_name,nReducers); //selecting the reducer using the already defined hash function.

        sprintf(msg.msgText, "output/MapOut/Map_%d/%s", i+1, entry->d_name);

        msg.msgType = i + 1;  //use reducerid as tag

        //memset(msg.mtext, '\0', MSGSIZE);

        int a = msgsnd(mid, &msg , MSGSIZE, 0); //

        if (a == -1)
        {
          //perror("Couldn't send the message");
          //exit(ERROR);     //error handling for msgsnd. return -1 if cannot send the message
        }

      }

    }

    //close directory

    closedir(dir);

  }

  //send end message to reducers

  for (int i=0; i<nReducers; i++)
 {
    msg.msgType = nReducers + 1;//--> use reducerID (i) as the tag
    sprintf(msg.msgText, "END");
    int b = msgsnd(mid, (void *) &msg, sizeof(msg.msgText), 0);

    if(b==-1)
    {
      //perror("Couldn't send the message");
      //exit(ERROR); //error handling return -1 if cannot send message

    }
  }

}

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		// printf("%d\n", i);
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}
