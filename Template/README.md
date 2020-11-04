test machine : CSELAB_machine_name
date : 11 / 04 / 2020
name : Babacar Diouf, Shubhavi Arya, Rezkath Awal
x500 : diouf006, aryax014, awalx003
Project Group Name: Project Group 32

- The purpose of your program: The purpose of our program is to take an input file and figure out how many duplicates of each word there are in
the file.   In order to accomplish this task, some functions use Inter-Process Communication in order to send data from the file to different 
processes.

- How to compile the program: 
	1) Use the command "make clean" in a shell.
	2) In that same shell, use the command "make mapreduce"
	3) In that same shell, use the command "make t1"

- What exactly your program does: Our program first reads individual strings from an input file and adds them to a string that represents
chunkData and cannot exceed 1024 bytes.  This process repeats until the input file is completely read.  Then, the master process 

   

- Any assumptions outside this document: 
	1) We have modified the main() function in mapreduce.c so that we can open a message queue, get a message ID and close the message queue.

- Contribution by each member of the team:
	-Contribution by Babacar Diouf: 
		-Wrote the shuffle() and getChunkData() functions.
		-Helped debug whole program.
	-Contribution by Shubhavi Arya: 
		-Wrote the parts of sendChunkData() function that does not involve string processing. 
		-Helped debug whole program.
	-Contribution by Rezkath Awal: 
		-Wrote the part of sendChunkData() function that does involve string processing. Wrote getInterData() function.  
		-Helped debug whole program.
		-Wrote this README file.
	

