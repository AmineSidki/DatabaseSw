import java.io.*;
import java.util.ArrayList;

public class Table {
    String Name;
    String cdb;
    ArrayList<Column> Columns;

    Table(String Table_Name , String current_db)
    {
        Name = Table_Name;
        cdb = current_db;
        Columns = new ArrayList<>();
    }
    public void insert(String[] args) throws IOException
    {
        //we verify if the data has the same number of columns as the table
        File table = new File(cdb + "/" + Name + ".dbt");
        ArrayList<String> Values = new ArrayList<>();
        for(int i = 4 ; i < args.length && !args[i].equals("$") ; i++)
        {
            Values.add(args[i]);
        }
        int r = 0 , GotCol;
        Column tmp ;
        Table th = new Table(Name,cdb);
        do{
            tmp = new Column(r);
            GotCol = tmp.GetCol(th);
            if(GotCol == 1)
            {
                Columns.add(tmp);
            }
            else{
                break;
            }
            r++;
        }while(GotCol == 1);

        int comma_counter = 0;
        for(int i = 0 ; i < Values.size() ; i++)
        {
            if((i+1) % 2 == 0 && !Values.get(i).equals(","))
            {
                System.out.println("Expected ',' near argument " + Values.get(i));
                System.exit(0);
            }
            else if (!Values.get(i).equals(",")) {
                try {
                    System.out.println(Columns.get(i - comma_counter).dt);
                    switch (Columns.get(i - comma_counter).dt) {
                        case Datatype.INT:
                            System.out.println("integer");
                            break;
                        case Datatype.FLOAT:
                            System.out.println("float");
                            break;
                        case Datatype.DATE:
                            System.out.println("date");
                            break;
                        default:
                            System.out.println("string");
                    }
                }catch(Exception e)
                {
                    //System.out.println(Values.get(i));
                }
            }
            else{
                comma_counter++;
            }
        }

        try(FileWriter fw = new FileWriter(table , true))
        {
            for(String e : Values)
            {
                if(!e.equals(","))
                {
                    fw.write(e);
                    fw.write("#$#");
                    System.out.println("Written to " + table.getAbsolutePath() +" "+ e);
                }
            }
            fw.write("\n");
            fw.close();
            System.out.println("Successfully inserted data !");
            System.exit(1);
        }catch (IOException ioe)
        {
            System.out.println("An error occurred while opening the database file .");
            System.exit(0);
        }
    }
    public void make(String[] args)
    {
        File dbd = new File(cdb + "/" + cdb +".dbd");
        //we then make a file named after the table , which will contain all the rows.
        File table = new File(cdb + "/" + Name + ".dbt");
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
        int nb_columns = 0;
        //for each datatype token , we verify if that datatype is supported.
        for (int index = 3; index < args.length && !args[index].equals("$"); index++) {
            if((index - 1) % 3 == 0)
            {
                ArrayList<String> DataTypes = new ArrayList<>();

                DataTypes.add("INT");
                DataTypes.add("FLOAT");
                DataTypes.add("CHAR");
                DataTypes.add("VARCHAR");
                DataTypes.add("DATE");

                boolean AcceptableType = false;

                for(String e : DataTypes)
                {
                    args[index] = args[index].toUpperCase();
                    if(e.equals(args[index]))
                    {
                        AcceptableType = true;
                        nb_columns++;
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
                err_del_table("Error : Expected ',' near argument " + curr.value , table);
            }
        }
        //if all is conform , we write the columns in the ".dbd" file of the database the
        //following way :
        // Col1$DataT1#Col2$DataT2#....#Coln$DataTn End of Line.
        try(FileWriter fw = new FileWriter(dbd , true)){
            fw.write(Name + ":" + nb_columns + ":");
            for (LL curr = columns; curr != null; curr = curr.next) {
                if (!curr.value.equals(",") || curr.next == null) {
                    fw.write(curr.value);
                    char sep;
                    if (curr.next != null && !curr.next.value.equals(",")) {
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
    private void err_inv_type(String type , String argumnt)
    {
        System.out.println("Error : Invalid type at argument : " + argumnt + " , expected " + type);
        System.exit(0);
    }
    private int Str_to_int(String str)
    {
        int out = 0;
        for(int i = 0; i < str.length() ; i++)
        {
            if(str.charAt(i) <= '9' && str.charAt(i) >= '0')
            {
                out = out * 10 + str.charAt(i) - '0';
            }
        }
        return out;
    }
    private boolean isInt(String str)
    {
        if(str.isEmpty())
        {
            return false;
        }
        for(int i = 0 ; i < str.length() ; i++)
        {
            if(str.charAt(i) > '9' || str.charAt(i) < '0')
            {
                return false;
            }
        }
        return true;
    }
    private boolean isFloat(String str)
    {
        if(str.isEmpty())
        {
            return false;
        }
        if(isInt(str))
        {
            return true;
        }
        int point_counter = 0 , i;
        for(i = 0; i < str.length() && ((str.charAt(i) <= '9' && str.charAt(i) >= '0') || (str.charAt(i) == '.' && point_counter == 0)) ; i++)
        {
            if(str.charAt(i) == '.')
            {
                point_counter++ ;
            }
        }
        return str.charAt(i - 1) != '.' && str.charAt(0) != '.' && i == str.length();
    }
    private boolean isDate(String str)
    {
        String[] splitted = str.split("/");
        return splitted.length != 3 || splitted[0].length() > 2 || splitted[1].length() > 2 || splitted[2].length() != 4;
    }
}
