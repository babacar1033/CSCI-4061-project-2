#include "utils.h"
#include <sys/msg.h> //in order to use message queues
#define PERM 0666//user, group, and others each have only read and write permissions
#define ERROR 0//for errors in the code

int mapperID;//global variable to use mapperID in functions contained in this file

char *getChunkData(int mapperID)
{
  //declare the same key as in the "sendchunkdata". To be used again to open the message queue.
  key_t key = ftok(".",5331326); // convert the pathname "key" and the reducer identifier to a System V IPC
  
  //error handling for ftok()
  if(key == -1){
  	perror("Key could not be generated");
  	exit(ERROR);
  }

  //declare a variable msg of type struct msgBuffer to represent the chunk
  struct msgBuffer msg;

  //open already existing message queue
  int mid = msgget(key, PERM| IPC_CREAT); // use PERM: user, groups and other can read and write. create the message queue if it's not done so yet
  
  //error handling for msgget()
  if(mid == -1){
    perror("Could not get message identifier");
    exit(ERROR);
  }

  int checkStatus = msgrcv(mid, &msg, sizeof(msg.msgText), mapperID, 0); //receive data from the master who was supposed to send the data to a specific mapperID
  if(checkStatus == -1){
  	//error handling for msgrcv
  	perror("Could not receive the data");
  	exit(ERROR);
  }else{
  	if(strlen(msg.msgText)==0){
  	// END message
  		return NULL;//pointer
  	}else{
  		char *v = (char *)malloc(sizeof(char)*(chunkSize+1));//dynamically allocate 1025 bytes of memory
		memset(v, '\0', chunkSize +1);
		strcpy(v, msg.msgText);
  		return v;
  	}
  }
}


// sends chunks of size 1024 to the mappers in round robin fashion
void sendChunkData(char *inputFile, int nMappers)
{
	//for message queue
	key_t key;
	key_t key2;
	int msgid, msgid2, ok, ok2;
	struct msgBuffer msg;
	struct msgBuffer msg2;
	char checkString;//character to hold what is returned from fgetc() later
	int characterCount = 0;//count how many bytes were returned from fgetc() so far
    char wholeString[chunkSize+1];//holds one string read from inputFile, the +1 is to account for '\0' outside of the 1024 byte count
    memset(wholeString, '\0', chunkSize+1);//to make sure wholeString is always null-terminated when adding individual characters to it
    int currentChunkSize = 0;
    memset(msg.msgText, '\0', MSGSIZE);
    	
    char word[100];//to hold string that will be used for next chunk
    	
	//generate unique key
	key = ftok(".", 5331326);
	
	//generate key for message queue for END messages
	key2 = ftok(".", 5436781);
	
	//error handling for ftok()
  	if(key == -1){
  		perror("Key could not be generated");
  		exit(ERROR);
	}
	
	if(key2 == -1){
  		perror("Key could not be generated");
  		exit(ERROR);
	}
	
	//creates a message queue
	msgid = msgget(key, PERM | IPC_CREAT);
	
	//create message queue 2
	msgid2 = msgget(key2, PERM | IPC_CREAT);
	
	//error handling for msgget()
    if(msgid == -1){
    	perror("Could not get message identifier");
    	exit(ERROR);
    }
    
    if(msgid2 == -1){
    	perror("Could not get message identifier");
    	exit(ERROR);
    }
	

	FILE* file = fopen (inputFile, "r");//open the input file for reading since we want to separate the file into chunks

	//error checking for fopen() --> see if the file pointer points to anything non-NULL
	if(file == NULL){
        perror("This file could not be opened");
        exit(ERROR);
    }

    mapperID = 0;//starting mapperID
    	
	//construct chunks of 1024 bytes each and send each chunk to a mapper
	checkString = fgetc (file);
	while (checkString != EOF){//check to see if reading has not reached at the end-of-file
		memset(word, '\0', 100);
	//	printf("Checkstring: %c\n", checkString);
		strncat(word, &checkString, 1);//concatenate the character just read to the word
	//	printf("word: %s\n", word);
		//characterCount++;//indicates that one character has been read for current chunk
	
		//concatenate what fgetc() returned until a whitespace, newline character, or EOF is encountered, which indicates that we are going to read a new string
		if((checkString != ' ') | (checkString != '\n') | (checkString != EOF)){
	//		printf("strlen(word) + currentChunkSize: %ld\n", (strlen(word) + currentChunkSize));
			if((strlen(word) + currentChunkSize) < chunkSize){
				currentChunkSize += strlen(word);//current size of chunkData
	//			printf("currentChunkSize: %d\n", currentChunkSize);
            	strcat(wholeString,word);
//            	printf("wholeString:%s\n", wholeString);
            	checkString = fgetc (file);//update position in the inputFile
  //          	printf("Checkstring in if-statement:%c\n", checkString);
            	continue;//then check to see if at EOF 
            }else{//then the chunk will be too big if you add the string --> so send the chunk and save the word for the next chunk
            	//send message (wholeString)
            	strcat(msg.msgText,wholeString);//concatenate wholeString to msg.msgText (which is the current mapper's chunkData)
//            	printf("chunkData: %s\n", msg.msgText);
            	msg.msgType = mapperID +1; //want to keep iterating through mappers until all the data is sent
            	// Move to next mapper
            	mapperID = (mapperID+1)%nMappers;
            	ok = msgsnd(msgid, (void *) &msg, sizeof(msg.msgText), 0);//send this chunk off to a mapper because the chunkData is full
  //          	printf("mapperID: %ld\n", msg.msgType);
            	memset(msg.msgText, '\0', MSGSIZE);//reset chunkData because we are going to add data to a new chunk
            	//reset currentChunkSize
            	currentChunkSize = 0;
            			
            	//replace wholeString with word
            	strcpy(wholeString, word);
            			
            }
        }
	}
	
	//at this point, we have reached EOF --> send out whole string even if whole string size is not chunkSize
	strcat(msg.msgText,wholeString);//concatenate wholeString to msg.msgText (which is the current mapper's chunkData)
	//printf("chunkData: %s\n", msg.msgText);
    msg.msgType = mapperID%nMappers +1;//iterate to next mapper
  //  printf("mapperID: %ld\n", msg.msgType);
    ok = msgsnd(msgid, (void *) &msg, sizeof(msg.msgText), 0);//send this chunk off to a mapper because the chunkData is full
    //at this point, we are done sending chunkData    		


	
	
	//send END message to mappers
	for (int i=1; i<=nMappers; i++){
//	printf("in sendChunk: i = %d\n", i);
		msg2.msgType = i;//use mapperID (which is "i") as the tag
		memset(msg.msgText, '\0', MSGSIZE);//reset the message text because we just want to send the END message
//		printf("msg2Text: %s\n", msg2.msgText);
		sprintf(msg2.msgText, "");
//		printf("msg2Text: %s\n", msg2.msgText);
		ok2 = msgsnd(msgid, (void *)&msg2, sizeof(msg2.msgText), 0);
//		printf("ok2: %d\n", ok2);
		if(ok2 == -1){
            perror("Could not send the data");
            exit(ERROR);
        }
	}
	//intf("AT THIS POINT"
	//int msgsnd(int msgid, const void *msgp, size_t msgsz, int msgflg)
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
    //for message queue
    struct msgBuffer one;
    int messageID;
    key_t key1;
    
    //generate unique key
    key1 = ftok(".", 5331326);
    
    //error handling for ftok()
    if(key1 == -1){
  	perror("Key could not be generated");
  	exit(ERROR);
    }

    //open message queue
    messageID = msgget (key1, PERM | IPC_CREAT);
    
    
    //error handling for msgget()
    if(messageID == -1){
    	perror("Could not get message identifier");
    	exit(ERROR);
    }
    
    
    //int msgsnd(int msgid, const void *msgp, size_t msgsz, int msgflg)
    
    //int msgrcv(int msgid, const void *msgp, size_t msgsz, long msgtype, int msgflg)
    
    
    

    int x = msgrcv (messageID, (void *)&one, sizeof (one.msgText), reducerID, 0);//receive the chunkData
    
    //error checking for msgrcv
    if(x == -1){
    	perror("Could not receive the data");
    	exit(ERROR);
    }
	
    //check for END message
    if(strlen(one.msgText)==0){
        return 0;//done reading data
    }else{
        strcpy(key, one.msgText);//copy one.msgText into key
        return 1;//there's more data to come
    }
}

void shuffle(int nMappers, int nReducers)

{
  char buff[chunkSize]; // a buffer to store the word filepath

  //declare the same key as in the "sendchunkdata". To be used again to open the message queue.
  key_t key = ftok(".", 5331326); // convert the pathname "key" and the reducer identifier to a System V IPC
  
  //error handling for ftok()
  if(key == -1){
  	perror("Key could not be generated");
  	exit(ERROR);
  }

  //declare a variable msg of type struct msgBuffer to represent the chunk
  struct msgBuffer msg;

  //open message queue
  int mid = msgget(key, PERM | IPC_CREAT); // use permission 0666 where user, groups and other can read and write. create the message queue if it's not done so yet
  
  //error handling for msgget()
    if(mid == -1){
    	perror("Could not get message identifier");
    	exit(ERROR);
    }

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

        msg.msgType = reducerId;  //use reducerId as tag

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
    sprintf(msg.msgText, "");
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
