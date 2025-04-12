# DatabaseSw
## About : 
This is my attempt to recreate a database management software from scratch (not really , I am still using java) .  
For the sake of simplicity , I don't plan to implement the totality of SQL commands so the project will be for learning purposes only of course.  

## Architecture :
For now , I am planning to have the main program get commands one by one in java , since I still haven't mastered GUIs in java I would prefer to use a TUI in C that will gather the entirety of the script then feed it line by line to the java main class, then and pipe the results to retrieve the status of the execution .
I will make sure to upload a schema to make things clearer.  

About the Main class , since it doesn't have a proper parser (for now) I don't think I will implement too intricate of a SELECT functionnality . Also , the tables and databases are in text format for now but I plan to change them into binaries .   

Each Database is stored in a folder that has the same name , a file named cdb.txt contains the current database . The folder initially has one file named after the database and has the ".dbd" extension , and at each new table created , a ".dbt" file having the same name as the table created is created and a new line is added in the dbd file that holds the different columns and their datatypes .  

## Progress :
The current implemented features are :
- Creating databases and tables
- Altering said databases and tables
- Inserting data into tables
- Checking datatype correspondance
- Checking for invalid values due to the user tinkering with the files (Needs updating)
- Dropping tables , databases or columns

The planned features to add :
- Relations in the database , primary / foreign keys constraints
- Selecting the data from a table
- Creating an interface (most likely going to be a TUI)
- Changing the files from text to binaries
- Changing the value validity check to use checksum  
##
This project took much longer than it should have initially mainly due to how many times I redid it (it was supposed to be in C and to have a vsCode-Like interface to write code but I soon realised it was kind of unrealistic to try and make this in less than 3 months , but it took longer regardless) , and to a lengthy break I took from it temporarly to focus on college projects and exams .  

I still did learn a lot looking into the documentations and public ressources about the different optimisations that say SQLite implements in order to significantly accelerate the reading / writing of data , though using java , reading and writing to chunks off of the disk is not gonna compensate the performance loss that is due to using the JVM to run it , besides , most of the operating systems use different file systems , so trying to implement them will not only be quite complex , but also a waste of time to me .
