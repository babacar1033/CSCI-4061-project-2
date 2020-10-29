#include "utils.h"

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

	//generate unique key
	key = ftok("project", 2);

	//creates a message queue
	msgid = msgget(key, PERM | IPC_CREAT);

	FILE* file = fopen (inputFile, "r");
	char line[1024];

	//construct chunks of 1024 bytes each and send
	//each chunk to a mapper
	while (fgets (line, sizeof(line), file)){
		chunk = getNextChunk(inputFile) // how to do this??
		ok = msgsnd(msgid, (void *) &chunk, mapperID) //where do you get mapperID

	}

	//send end message to mappers
	for (int i=0; i<mapperID; i++){
		msg.mtype = 111;
		memset(msg.mtext, '\0', MSGSIZE);
		sprintf(msg.mtext, "END");
		ok2 = msgsnd(msgid, (void *)&msg, MSGSIZE, mapperID);
	}

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