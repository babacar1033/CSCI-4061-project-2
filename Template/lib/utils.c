#include "utils.h"
#include <stdbool.h>//to use boolean variable
#include <sys/msg.h>//-->in order to use message queues
#define PERM 0666//--> user, group, and others each have only read and write permissions
#define ERROR 0//for errors in the code

//Macros that indicate if cases in sendChunkData's helper function
#define LESS_THAN_CHUNKSIZE 1
#define EQUAL_CHUNKSIZE 2
#define MORE_THAN_CHUNKSIZE 3
int mapperID;//global variable to use mapperID in functions in this file

char *getChunkData(int mapperID)

{
  //declare the same key as in the "sendchunkdata". To be used again to open the message queue.
  key_t key = ftok(".",5331326); // convert the pathname "key" and the reducer identifier to a System V IPC

  //declare a variable msg of type struct msgBuffer to represent the chunk
  struct msgBuffer msg;

  //open message queue
  int mid = msgget(key, PERM| IPC_CREAT); // use PERM where user, groups and other can read and write. create the message queue if it's not done so yet

  //error handling
  if(mid == -1)
  {
    perror("Could not get message identifier");
    exit(ERROR); //return -1 and set errno
  }

  int checkStatus = msgrcv(mid, &msg, sizeof(msg.msgText), mapperID, 0); //receive data from the master who was supposed to send a specific mapperID
  if(checkStatus == -1){
  	//error handling
  	perror("Could not receive the data");
  	exit(ERROR);
  }else{
  	char *v = malloc(MSGSIZE);
  	strcpy(v, msg.msgText);
  	return v;
  }
  
}

//helper function for sendChunkData()
int compareToChunkSize(int currentChunkSize, int characterCount, char *wholeString, int nMappers, struct msgBuffer msg){
    key_t key;
    int msgid, ok;
    
    //generate unique key
    key = ftok(".", 5331326);

    //creates a message queue
    msgid = msgget(key, PERM | IPC_CREAT);
    
    if(currentChunkSize < chunkSize){
        strcat(msg.msgText,wholeString);//concatenate wholeString to msg.msgText (which is chunkData thus far)
        strcat(msg.msgText," ");//add whitespace character at end of msg.msgText
        printf("First message%s", msg.msgText);
        memset(wholeString, '\0', chunkSize);//reset "wholeString" because we are going to read a new whole string from inputFile
        return LESS_THAN_CHUNKSIZE;
    }else if(currentChunkSize > chunkSize){
    	bool yes = true;
    	int i;
    	for(i = 0; i < MSGSIZE; i++){
    		if(msg.msgText[i] != '\0'){
    			yes = false;//memset() was not just called on msg.msgText
    		}
    	}
        if(true){//msg.msgText just consists of null terminators
            //at this point, we just started a new chunk to add chunkData to.  Also, in the else if, the size of the whole string exceeds 1024 bytes
            //so we cannot add it to the chunk
            perror("Word in chunk is too long - cannot be sent without being separated.");
            exit(ERROR);
        }
        
        //At this point, DO NOT concatenate whole string to "msg.msgText" because according to the else if case, the size of the whole string exceeds 1024 bytes
        //we will add this string to the next chunk. 
        printf("Second message%s", msg.msgText);
        ok = msgsnd(msgid, (void *) &msg, sizeof(msg.msgText), mapperID);//send this chunk (without the long whole string) off to a mapper
        if(ok == -1){
            perror("Could not send the data HERE2");
            exit(ERROR);
        }
        
        
        if(mapperID < nMappers){
            mapperID++;//go to the next mapper
        }
        
        return MORE_THAN_CHUNKSIZE;
    }else{
        strcat(msg.msgText,wholeString);//concatenate wholeString to msg.msgText (which is chunk data thus far)
        strcat(msg.msgText," ");//add whitespace character at end of msg.msgText
        printf("Third message%s", msg.msgText);
        
        ok = msgsnd(msgid, (void *) &msg, sizeof(msg.msgText), mapperID);//send this chunk off to a mapper because the chunkData is full
        if(ok == -1){
            perror("Could not send the data HERE1");
            exit(ERROR);
        }
        
        if(mapperID < nMappers){
            mapperID++;//go to the next mapper
        }
        
        //reset characterCount, currentChunkSize, and "msg.msgText" (chunk data) because we are to start a new chunk
        characterCount = 0;
        currentChunkSize = 0;
        memset(msg.msgText, '\0', MSGSIZE);
        
        memset(wholeString, '\0', chunkSize);//reset "wholeString" because we are going to read a new whole string from inputFile
        return EQUAL_CHUNKSIZE;
    }
}


// sends chunks of size 1024 to the mappers in round robin fashion
void sendChunkData(char *inputFile, int nMappers)

{
	//for message queue
	key_t key;
	int msgid, ok, ok2;
	struct msgBuffer msg;
	char checkString;//character to hold what is returned from fgetc() later
	int characterCount = 0;//count how many bytes were returned from fgetc() so far
    	char wholeString[chunkSize];//holds one string read from inputFile
    	//memset(wholeString, '\0', chunkSize);//to make sure it is always null-terminated
    	int currentChunkSize = 0;
    	

	//generate unique key
	key = ftok(".", 5331326);

	//creates a message queue
	msgid = msgget(key, PERM | IPC_CREAT);

	FILE* file = fopen (inputFile, "r");//open the input file for reading since we want to separate the file into chunks

	//error checking for fopen() --> see if the file pointer points to anything non-NULL
	if(file == NULL){
        	perror("This file could not be opened");
        	exit(ERROR);
    	}

    	mapperID = 1;//starting mapperID

	//construct chunks of 1024 bytes each and send each chunk to a mapper
	checkString = fgetc (file);
	while (checkString != EOF){//check to see if reading has not reached at the end-of-file
		printf("Got here");
		characterCount++;//indicates that one character has been read for current chunk
		if(checkString != ' '){//concatenate what fgetc() returned until a whitespace is encountered, which indicates that we are going to read a new string
            		strncat(wholeString,&checkString, 1);
		}else{//at this point, a whitespace has been encountered, which means we read one whole string from inputFile
            		//add size of a whole string
            		currentChunkSize += (characterCount-1);//subtract 1 from characterCount to account for whitespace --> don't add whitespace to the chunk yet 
            		int x = compareToChunkSize(currentChunkSize, characterCount, wholeString, nMappers, msg);
            		if(x == MORE_THAN_CHUNKSIZE){//means that a whole string that is read from inputFile has more than 1024 bytes
                		memset(msg.msgText, '\0', MSGSIZE);//reset chunkData because we are going to add data to a new chunk
                		compareToChunkSize((characterCount-1), characterCount, wholeString, nMappers, msg);
            		}
		}
	    	checkString = fgetc (file);//update position in the inputFile
	}

	//send end message to mappers
	for (int i=1; i<=nMappers; i++){
		msg.msgType = i;//use mapperID (which is "i") as the tag
		memset(msg.msgText, '\0', MSGSIZE);//reset the message text because we just want to send the END message
		sprintf(msg.msgText, "END");
		ok2 = msgsnd(msgid, (void *)&msg, sizeof(msg.msgText), i);
		if(ok2 == -1){
            		perror("Could not send the data HERE");
            		exit(ERROR);
        	}
	}
	/*

    	//--> is this for the ACK??
	for (int i =0; i<nMappers; i++){
		wait(msgid);
	}
	*/

	fclose(file);
	//got to end of sendChunkData
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
    //for message queue
    struct msgBuffer one;
    int messageID;
    key_t key1;
    
    //generate unique key
    key1 = ftok(".", 5331326);

    //open message queue
    messageID = msgget (key1, PERM | IPC_CREAT);

    key1 = msgrcv (messageID, (void *)&one, sizeof (one.msgText), reducerID, 0);//receive the chunkData
    
    //error checking
    if(key1 == -1){
    	perror("Could not receive the data");
    	exit(ERROR);
    }

    //check for END message
    if(one.msgText == "END"){
        return 0;//done reading data
    }else{
        strcpy(key, one.msgText);//copy one.msgText into key
        return 1;//there's more data to come
    }
}

void shuffle(int nMappers, int nReducers)

{
  char buff[1024]; // a buffer to store the word filepath

  //declare the same key as in the "sendchunkdata". To be used again to open the message queue.
  key_t key = ftok(".", 5331326); // convert the pathname "key" and the reducer identifier to a System V IPC

  //declare a variable msg of type struct msgBuffer to represent the chunk
  struct msgBuffer msg;

  //open message queue
  int mid = msgget(key, PERM | IPC_CREAT); // use permission 0666 where user, groups and other can read and write. create the message queue if it's not done so yet

  // preparation of traversing the directory of each Mapper and send the word filepath to the reducers
  struct dirent *entry;


  for (int i = 0; i < nMappers; i += 1)
  {
    sprintf(buff, "output/MapOut/Map_%d", i+1); //copy word filepath to buffer

    DIR *dir = opendir(buff);  //open mapOutdirectory

    //error checking --> check if directory exists.
    if (dir == NULL)
    {
      perror("The path passed is invalid");
      exit(ERROR);
    }


    while (entry = readdir(dir)) //traverse the directory of 1 mapper
    {
      if (entry->d_type == DT_REG) //verify if the type entry is pointing to is a file. if so select the reducer using a hash function

      {
        int reducerId = 1 + hashFunction(entry->d_name,nReducers); //selecting the reducer using the already defined hash function.

        sprintf(msg.msgText, "output/MapOut/Map_%d/%s", i+1, entry->d_name);//create file path using mapperID and file name

        msg.msgType = i + 1;  //use reducerID as tag

        //memset(msg.mtext, '\0', MSGSIZE);

        int a = msgsnd(mid, &msg , MSGSIZE, 0);

	//error handling for msgsnd. 
        if (a == -1)
        {
          perror("Couldn't send the message");
          exit(ERROR);     
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

    //error handling for msgsnd()
    if(b==-1)
    {
      perror("Couldn't send the message");
      exit(ERROR);  

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
