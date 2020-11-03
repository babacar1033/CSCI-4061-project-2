#include "utils.h"
#include <sys/msg.h>//-->in order to use message queues
#define PERM 0666//--> user, group, and others each have only read and write permissions
#define ERROR 0//for errors in the code

char *getChunkData(int mapperID)

{

  //declare the same key as in the "sendchunkdata". To be used again to open the message queue.

  key_t key = ftok("project",2); // convert the pathname "key" and the reducer identifier to a System V IPC

  //declare a variable msg of type struct msgBuffer to represent the chunk4

  struct msgBuffer msg;

  //open mesage queue

  int mid = msgget(key, PERM| IPC_CREAT); // use permission 0644 where user, groups and other can read and write. create the message queue if it's not done so yet

  if(mid == -1)
  {
    return -1; //error handling. return -1 and set errno
  }

  msgrcv(mid, &msg, sizeof(msg.msgText), mapperID, 0); //receive data from the master who was supposed to send a specific mapperID


}
// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers)

{

  key_t key;
  int msgid, ok, ok2;
  message msg;
  int checkString;//--> I declared this here because I do not believe you declared it somewhere
  int characterCount = 0;//--> use this to see if a word is shared between the end of one chunk and the beginning of the next chunk
  char wholeString[chunkSize];//--> temporary storage
  memset(wholeString, '\0', chunkSize);//--> to make sure it is always null-terminated
  int currentChunkSize = 0;

  //generate unique key
  key = ftok("project", 2);//--> use your x500 as the second argument of ftok(): a TA talks about this in the most recent Canvas announcement

  //creates a message queue
  msgid = msgget(key, PERM | IPC_CREAT);

  FILE* file = fopen (inputFile, "r");
  char line[chunkSize];//--> chunkSize Macro is defined in "utils.h"

  //--> error checking for fopen() --> see if the file pointer points to anything non-NULL
  if(file == NULL){
    printf("This file could not be opened");
    exit(ERROR);
  }

  mapperID = 0;

  //construct chunks of 1024 bytes each and send
  //each chunk to a mapper
  while ((checkString = fgetc (file)) != EOF){//--> check to see if the pointer is not at the end-of-file
    characterCount++;//--> a character has been read for current chunk
    if(checkString != ' '){
      strcat(wholeString,checkString);//--> concatenate to wholeString
    }else{//--> checkString != ' '
      //--> if at this point, we read one whole string in inputFile

      //--> add size of a whole string
      currentChunkSize += (characterCount-1);//--> the "-1" is to account for the space
      if(currentChunkSize <= chunkSize){
        strcat(line,wholeString);//--> concatenate wholeString to line (which is chunk thus far)
        strcat(line,checkString);//--> whitespace character
        if(currentChunkSize == chunkSize){
          ok = msgsnd(msgid, (void *) &line, mapperID)
          if(mapperID < nMappers){
            mapperID++;
          }
          //--> reset characterCount, currentChunkSize, and "line" (chunk)
          characterCount = 0;
          currentChunkSize = 0;
          memset(line, '\0', chunkSize);//--> reset "line" (chunk)
        }
        memset(wholeString, '\0', chunkSize);//--> so reset "wholeString"
      }else if(currentChunkSize > chunkSize){
        //--> then DO NOT concatenate whole string to "line"
        ok = msgsnd(msgid, (void *) &line, mapperID)
        if(mapperID < nMappers){
          mapperID++;
        }
        //--> reset currentChunkSize (because we are making a new chunk), and reset "line" (chunk)
        currentChunkSize = 0;
        memset(line, '\0', chunkSize);//--> reset "line" (chunk)

        currentChunkSize += (characterCount-1);//--> at this point, I want to go back to line 53 --> I'm not sure how to implement this
      }
    }
  }

  //send end message to mappers
  for (int i=0; i<nMappers; i++){
    msg.mtype = i+1;//--> use mapperID (i) as the tag
    memset(msg.mtext, '\0', MSGSIZE);
    sprintf(msg.mtext, "END");
    ok2 = msgsnd(msgid, (void *)&msg, MSGSIZE, i);
  }

  //--> is this for the ACK??
  for (int i =0; i<nMappers; i++){
    wait(msgid);
  }

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
    message one;
    int messageID;
    key_t key1;

    key1 = ftok(key, awalx003);//file path is the "key"

    //open message queue
    messageID = msget (key1, PERM | IPC_CREAT);

    key1 = msgrcv (messageID, (void *)&one, sizeof (one.mtext), reducerID, 0);

    //check for END message
    if(one.mtext == "END"){
        *key = key;
        return 0;//done reading data
    }else{
        *key = key;
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
      printf("The path passed is invalid");
      return -1;
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
          return -1;     //error handling for msgsnd. return -1 if cannot send the message
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
      return -1; //error handling return -1 if cannot send message

    }
  }

  //wait for ACK from the reducers for END notification

//  for (int i=0; i<nReducers; i++)
//  {
//    wait(mid);
//  }

  

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