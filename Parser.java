import java.io.*;
import java.util.*;

class LL{
    String value;
    LL next;
    LL(String str)
    {
        value = str;
        next = null;
    }
    void add_node(String str)
    {
        if(next == null)
        {
            next = new LL(str);
        }
        else{
            LL curr = next ;
            while(curr.next != null)
            {
                curr = curr.next;
            }
            curr.next = new LL(str);
        }
    }
}

public class Parser {

    public static void main(String[] args) {
        try {
            String current_db = "";
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
            }catch (IOException ioe)
            {
                File cdb = new File("cdb.txt");
                boolean b = cdb.createNewFile();
                if(!b)
                {
                    System.out.println("failure to create cdb file !");
                    return;
                }
            }
                File dbd;
                String Command = args[0];
                Command = Command.toUpperCase();
                switch (Command) {
                    case "CDB":
                        if(current_db.isEmpty())
                        {
                            System.out.println("No Current database .");
                            return;
                        }
                        System.out.println(current_db);
                        break;
                    case "SELECT":
                        if(args[1].toUpperCase().equals("DATABASE"))
                        {
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
                                File db = new File(args[2]);
                                if(db.exists())
                                {
                                    System.out.println("A database with a similar name already exists !");
                                    return;
                                }
                                boolean i = db.mkdir();
                                if (i) {
                                    dbd = new File(args[2].concat("/" + args[2] + ".dbd"));
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
                                    return;
                                }
                                break;
                            case "TABLE":
                                if(current_db.isEmpty())
                                {
                                    System.out.println("No current database !");
                                    return;
                                }
                                dbd = new File(current_db + "/" + current_db +".dbd");
                                File Table = new File(current_db + "/" + args[2] + ".dbt");
                                try {
                                    boolean b = Table.createNewFile();
                                    if(!b)
                                    {
                                        System.out.println("Failure to create table !");
                                    }
                                } catch (IOException e) {
                                    System.out.println("Failure to create table !");
                                }
                                LL columns = null;
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
                                                return;
                                        }
                                    }
                                    if (columns == null) {
                                        columns = new LL(args[index]);
                                    } else {
                                        columns.add_node(args[index]);
                                    }
                                }
                                int index = 1;
                                for (LL curr = columns; curr != null; curr = curr.next , index++) {
                                    if (index % 3 == 0 && !curr.value.equals(",")) {
                                            System.out.println("Expected ',' near argument " + curr.value + " " + index + args[3+index]);
                                            return;
                                    }
                                }
                                try(FileWriter fw = new FileWriter(dbd , true)){
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
