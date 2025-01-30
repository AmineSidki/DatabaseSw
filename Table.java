import java.io.*;
import java.util.ArrayList;

public class Table {
    String Name;
    String[] Columns;
    String[] DataTypes;

    Table(String Table_Name)
    {
        Name = Table_Name;
    }
    public void make(String current_db,String[] args) throws IOException
    {
        File dbd = new File(current_db + "/" + current_db +".dbd");
        //we then make a file named after the table , which will contain all the rows.
        File table = new File(current_db + "/" + args[2] + ".dbt");
        try {
            boolean b = table.createNewFile();
            if(b == false)
            {
                System.out.println("Table Already exists !");
                return;
            }
        } catch (IOException e) {
            System.out.println("Failure to create table !");
        }
        //We make a linked list for the following tokens
        LL columns = null;
        //for each datatype token , we verify if that datatype is supported.
        for (int index = 3; index < args.length && !args[index].equals("$"); index++) {
            if((index - 1) % 3 == 0)
            {
                ArrayList<String> DataTypes = new ArrayList<String>();

                DataTypes.add("INT");
                DataTypes.add("FLOAT");
                DataTypes.add("CHAR");
                DataTypes.add("VARCHAR");
                DataTypes.add("DATE");

                boolean AcceptableType = false;

                for(String e : DataTypes)
                {
                    if(e.equals(args[index].toUpperCase()))
                    {
                        AcceptableType = true;
                    }
                }

                if(AcceptableType == false)
                {
                    err_del_table("Unsupported data Type ! : " + args[index].toUpperCase() , table);
                }

            }
            //if they are , we make a new node in the linked list, if not , we just exit the program.
            if (columns == null) {
                columns = new LL(args[index]);
            } else {
                columns.add_node(args[index]);
            }
            //we check if the last token is the end sequence : '$'
            if(index == args.length - 1)
            {
                err_del_table("Error : Expected '$' to end the sequence !" , table);
            }
        }
        // we check if the input is formatted the following way :
        // CREATE TABLE table_name col1 DataT1 , col2 DataT2 , ... , coln DataTn $
        int index = 1;
        for (LL curr = columns; curr != null; curr = curr.next , index++) {
            if (index % 3 == 0 && !curr.value.equals(",")) {
                err_del_table("Expected ',' near argument " + curr.value + " " + index + args[3+index] , table);
            }
        }
        //if all is conform , we write the columns in the ".dbd" file of the database the
        //following way :
        // Col1$DataT1#Col2$DataT2#....#Coln$DataTn End of Line.
        try(FileWriter fw = new FileWriter(dbd , true)){
            fw.write(args[2] + ":");
            for (LL curr = columns; curr != null; curr = curr.next) {
                if (!curr.value.equals(",") || curr.next == null) {
                    fw.write(curr.value);
                    char sep;
                    if ( curr.next != null && !curr.next.value.equals(",")) {
                        sep = '$';
                    } else {
                        sep = '#';
                    }
                    fw.write(sep);
                }
            }
            fw.write('\n');
        }catch(IOException ioe)
        {
            err_del_table("An error occurred while creating the table , please try again ." , table);
        }
        System.out.println("Successfully created Table !");
    }
    private void err_del_table(String err , File table)
    {
        System.out.println(err);
        if(table.delete() != false)
        {
            System.out.println("Exiting the program ..");
        }else{
            System.out.println("Failure to delete corrupt table , retrying ..");
            boolean b ;
            do{
                b = table.delete();
            }while(b == false);
        }
        System.exit(0);
    }
}
