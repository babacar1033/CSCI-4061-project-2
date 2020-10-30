#include "utils.h"
#include <sys/msg.h>//-->in order to use message queues
#define PERM 0666//--> user, group, and others each have only read and write permissions
#define ERROR 0//for errors in the code

//msgctl(msgid, IPC_RMID, NULL); --> Do at beginning of program

char *getChunkData(int mapperID) {
}

// sends chunks of size 1024 to the mappers in RR fashion
typedef struct msg_buffer {
    long mtype;
    char mtext[MSGSIZE];
} message;

void sendChunkData(char *inputFile, int nMappers) {
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
		msg.mtype = i;//--> use mapperID (i) as the tag
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
    key_t key;

    key = ftok(key, awalx003);//file path is the "key"

    //open message queue
    mid = msget (key, PERM | IPC_CREAT);

    key = msgrcv (messageID, (void *)&one, sizeof (one.mtext), reducerID, 0);


}


void shuffle(int nMappers, int nReducers) {
}

// check if the character is valid for a word
int validChar(char c){
	return ((tolower(c) >= 'a') && tolower(c <='z')) ||
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
