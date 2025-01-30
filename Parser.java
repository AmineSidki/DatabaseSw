import java.io.*;

public class Parser {

    static String GetCDB() throws IOException
    {
        String current_db = "";
        //we read from the cdb file that holds the name of the last selected database
        try(FileReader fr = new FileReader("cdb.txt"))
        {
            boolean CanRead = true;
            while(CanRead)
            {
                int ch = fr.read();
                if(ch > 0)
                {
                    current_db += (char)ch;
                }
                else{
                    CanRead = false;
                    fr.close();
                }
            }
            //if there was no file we create one
        }catch (IOException ioe)
        {
            File cdb = new File("cdb.txt");
            boolean b = cdb.createNewFile();
            if(!b)
            {
                System.out.println("failure to create cdb file !");
                return "";
            }
        }
        return current_db;
    }
    static void mkdb(String[] args)
    {
        //get the database name from the input
        File db = new File(args[2]);
        //testing if the name is unique in the directory
        if(db.exists())
        {
            System.out.println("A database with a similar name already exists !");
            return;
        }
        //if not , we make a folder comporting the name of the db
        boolean i = db.mkdir();
        if (i) {
            //then we make a new file , a ".dbd" file holding the db columns and their datatypes
            File dbd = new File(args[2].concat("/" + args[2] + ".dbd"));
            try {
                boolean b = dbd.createNewFile();
                if(!b)
                {
                    System.out.println("An error has occurred while creating the table or it already exists !");
                }
            } catch (IOException e) {
                System.out.println("An Error Occurred while Creating the database data file !");
            }
            System.out.println("Successfully created database !");
        } else {
            System.out.println("An Error Occurred while creating the database");
        }
    }
    static void mktbl(String current_db,String[] args)
    {
        //we check if we have an active db
        if(current_db.isEmpty())
        {
            System.out.println("No current database !");
            return;
        }
        //if yes , we get the dbd file of the db located at :
        //          current_dir/db_name/db_name.dbd
        File dbd = new File(current_db + "/" + current_db +".dbd");
        //we then make a file named after the table , which will contain all the rows.
        File Table = new File(current_db + "/" + args[2] + ".dbt");
        try {
            boolean b = Table.createNewFile();
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
                switch(args[index].toUpperCase())
                {
                    case "INT" :
                        //still under works
                        break;
                    case "CHAR" :
                        //still under works
                        break;
                    case "VARCHAR" :
                        //still under works
                        break;
                    case "FLOAT" :
                        //still under works
                        break;
                    case "DATE" :
                        //still under works
                        break;
                    case "STRING" :
                        //still under works
                        break;
                    default:
                        System.out.println("Unresolved datatype : " + args[index].toUpperCase());
                        System.exit(0);
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
                System.out.println("Error : Expected '$' to end the sequence !");
                System.exit(0);
            }
        }
        // we check if the input is formatted the following way :
        // CREATE TABLE table_name col1 DataT1 , col2 DataT2 , ... , coln DataTn $
        int index = 1;
        for (LL curr = columns; curr != null; curr = curr.next , index++) {
            if (index % 3 == 0 && !curr.value.equals(",")) {
                System.out.println("Expected ',' near argument " + curr.value + " " + index + args[3+index]);
                System.exit(0);
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
            System.out.println("An error occurred while creating the table , please try again .");
        }
    }

    public static void main(String[] args) {
        //catching the case where the input is empty
        try {
            //Get current database (if there is) from the cdb.txt file
            //if the file doesn't exist in the directory , create one .
            String current_db = GetCDB();
                //the dbd file is the one where all the database's tables' data (column_names and data
                // types) are stored.
                File dbd;
                //command is the first string after java Parser
                String Command = args[0];
                Command = Command.toUpperCase();
                switch (Command) {
                    case "CDB":
                        //This command outputs the name of the current database
                        if(current_db.isEmpty())
                        {
                            System.out.println("No Current database .");
                            return;
                        }
                        System.out.println(current_db);
                        break;
                    case "SELECT":
                        //This command lets you select a database ; its also placeholder
                        //for selecting columns from tables.
                        if(args[1].equalsIgnoreCase("DATABASE"))
                        {
                            //testing if the chosen database exists in the directory
                                File db = new File(args[2]);
                                if(db.exists())
                                {
                                    current_db = args[2];
                                    PrintWriter pw = new PrintWriter("cdb.txt");
                                    pw.print(current_db);
                                    pw.close();
                                }
                                else{
                                    System.out.println("No such database");
                                }
                        }
                        else{
                            System.out.println("Under works");
                            return;
                        }
                        break;
                    case "CREATE":
                        Command = args[1].toUpperCase();
                        switch (Command.toUpperCase()) {
                            case "DATABASE":
                                    mkdb(args);
                                break;
                            case "TABLE":
                                    mktbl(current_db,args);
                                break;
                            default:
                                System.out.println("Invalid Syntax");
                        }
                        break;
                    case "INSERT":
                        System.out.println("insert");
                        break;
                    case "ALTER":
                        System.out.println("alter");
                        break;
                    case "QUIT":
                        //we quit the current open database , ie : we erase the cdb.txt file.
                        PrintWriter pw = new PrintWriter("cdb.txt");
                        pw.close();
                        return;
                    default:
                        System.out.println("Invalid syntax");
                        return;

                }
        } catch (Exception e) {
            System.out.println("No Arguments provided !");
        }
    }
}
