import dbs.*;

import java.io.*;
import java.util.*;

public class Parser {

    static int CDB(String current_db)
    {
        //we check if we have an active db
        if(current_db.isEmpty())
        {
            System.out.println("No current database !");
            return 0;
        }
        //if yes , we return 1.
       return 1;
    }
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
    static void mkdb(String[] args) throws IOException
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
            selectdb(args[2]);
        } else {
            System.out.println("An Error Occurred while creating the database");
        }
    }
    static String selectdb(String dbname) throws IOException
    {
        String current_db = "";
        //testing if the chosen database exists in the directory
        File db = new File(dbname);
        if(db.exists())
        {
            current_db = dbname;
            PrintWriter pw = new PrintWriter("cdb.txt");
            pw.print(current_db);
            pw.close();
            System.out.println("Current Database : " + current_db);
        }
        else{
            System.out.println("No such database");
        }
        return current_db;
    }
    
    public static void main(String[] args) {
        //catching the case where the input is empty

        if(!args[args.length - 1].equals("$"))
        {
            System.out.println("Error : Missing exit sequence '$' ");
            return;
        }

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
                        System.out.println("Current Database : " + current_db);
                        break;
                    case "SELECT":
                        //This command lets you select a database ; its also placeholder
                        //for selecting columns from tables.
                        if(args[1].equalsIgnoreCase("DATABASE"))
                        {
                            current_db = selectdb(args[2]);
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
                                if(CDB(current_db) != 0) {
                                    if(args[2].toUpperCase().charAt(0) < 'A' || args[2].toUpperCase().charAt(0) > 'Z')
                                    {
                                        System.out.println("Error : Table Name must start with a letter !");
                                        System.exit(0);
                                    }
                                    Table table = new Table(args[2],current_db);
                                    table.make(args);
                                }
                                break;
                            default:
                                System.out.println("Invalid Syntax");
                        }
                        break;
                    case "INSERT":
                        if((args[1]+args[3]).equalsIgnoreCase("INTOVALUES"))
                        {
                            if(CDB(current_db) == 0) {
                                System.out.println("Error : No current database .");
                            }
                            if(!args[args.length-1].equals("$"))
                            {
                                System.out.println("Error : Expected '$' to end the sequence !");
                                System.exit(0);
                            }
                            Table table = new Table(args[2] , current_db);
                            table.insert(args);
                        }else{
                            System.out.println("Error : incorrect syntax , expected INTO '" + args[2] +"' VALUES ...");
                            System.exit(0);
                        }
                        break;
                    case "ALTER":
                        Table table = null;
                        switch (args[1].toUpperCase())
                        {
                            case "DATABASE" :
                                if(args.length < 6 ||!(args[2].toUpperCase() + args[3].toUpperCase()).equals("DROPTABLE") || !args[5].equals("$"))
                                {
                                    System.out.println("Error : Invalid Syntax");
                                    System.exit(0);
                                }
                                table = new Table(args[4], current_db);
                                table.drop();

                                break;
                            case "TABLE" :
                                if(!args[4].equalsIgnoreCase("Column"))
                                {
                                    System.out.println("Error : Invalid Syntax " + args[4]);
                                    return;
                                }
                                switch(args[3].toUpperCase())
                                {
                                    case "ADD":
                                        if(args.length < 7)
                                        {
                                            System.out.println("Error : Invalid argument size !");
                                            return;
                                        }
                                        table = new Table(args[2] , current_db);
                                        table.getTable();

                                        try{
                                            Column col = new Column(args[5] , args[6]);
                                            boolean b = table.addCol(col);
                                            
                                            if(!b)
                                            {
                                                System.out.println("Error : Failure to add column !");
                                            }
                                        }catch(IOException ioe)
                                        {
                                            System.out.println("Error : Failure to add column !");
                                            return;
                                        }
                                        break;
                                    case "DROP":
                                        table = new Table(args[2] , current_db);

                                        table.getTable();
                                        for(Column c : table.Columns)
                                        {   
                                            if(c.Name.equals(args[5]))
                                            {
                                               boolean wasDropped = c.drop(table);
                                               if(wasDropped)
                                               {
                                                   System.out.println("Successfully dropped column !");
                                                   return;
                                               }
                                                System.out.println("Error : an error occurred while dropping column : " + args[2]);
                                            }
                                        }
                                        break;
                                    default:
                                        System.out.println("Error : Invalid Syntax");
                                        return;
                                }
                                break;
                            default:
                                System.out.println("Invalid Syntax");
                        }
                        break;
                    case "QUIT":
                        //we quit the current open database , ie : we empty the cdb.txt file.
                        PrintWriter pw = new PrintWriter("cdb.txt");
                        pw.close();
                        System.out.println("No Current Database .");
                        return;
                    default:
                        System.out.println("Invalid syntax");
                }
        } catch (Exception e) {
            System.out.println("No Arguments provided ! " +  e);
        }
    }
}