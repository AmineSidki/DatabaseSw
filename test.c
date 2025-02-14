//This is a program to test out the database , with dropping columns ,
//and once I implement it , adding them .

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main()
{
    // 1st step : create a database and a table , with fields Name varchar , 
    //Surname varchar , CC float/int $

    char* TableName = malloc(100);
    
    gets(TableName);
    TableName = realloc(TableName , strlen(TableName));
    
    char buff[2000][100];
    for(int i = 0 ; i < 2000 ; i ++)
    {
        sprintf(buff[i] , "java Parser insert into %s values Name%d , Surname%d , %d $" , TableName , i , i , i);
    }

    for(int i = 0 ; i < 2000 ; i ++)
    {
        system(buff[i]);
    }

    return 0;

}   