import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

enum Datatype{
    CHAR,
    VARCHAR,
    INT,
    FLOAT,
    DATE
}
public class Column {
    int rank;
    String Name;
    Datatype dt;
    private boolean isPrimary = false;

    Column(int r)
    {
        rank = r;
    }
    Column(String Name)
    {
        this.Name = Name;
    }
    public int getCol(Table table)throws IOException
    {
        String dbd_fileName = table.cdb + "/" + table.cdb + ".dbd";
        String[] dbd_rows = ReadFile(dbd_fileName).split("\n");
        String table_row = "";
        int index = 0;
        String[] tmp ;
        do
        {
            tmp = dbd_rows[index].split(":");
            if(tmp[0].equals(table.Name))
            {
                table_row = tmp[2];
                break;
            }
            index++;
        }while(index < dbd_rows.length);

        if(table_row.isEmpty())
        {
            System.out.println("Error : Invalid Table Name !");
            return 0;
        }
        int l = table_row.split("#").length;
        if(rank > l)
        {
            return 0;
        }
        String[] Col_splitted = table_row.split("#");
        if(rank >= Col_splitted.length)
        {
            return 0;
        }
        Col_splitted = Col_splitted[rank].split("\\$");
        Name = Col_splitted[0];

        dt = Datatype.valueOf(Col_splitted[1].toUpperCase());
        return 1;
    }
    public boolean drop(Table table)throws IOException
    {
        String[] Tables = ReadFile(table.cdb + "/" + table.cdb + ".dbd").split("\n");
        String t = null;
        for(String e : Tables)
        {
            if(e.split(":")[0].equals(table.Name))
            {
                t = e.split(":")[2];
            }
        }
        if(t == null)
        {
            System.out.println("Error : No such column in table " + table.Name);
            System.exit(0);
        }
        try (FileWriter fw = new FileWriter(table.cdb + "/" + table.cdb + ".dbd" , false)){
            int r = 0;
            //deleting the column from the dbd file
            for(String e : Tables)
            {
                //searching the dbd file for the table row
                if(!e.split(":")[0].equals(table.Name))
                {
                    fw.write(e);
                    if(!e.equals(Tables[Tables.length - 1]))
                    {
                        fw.write("\n");
                    }
                }
                //when found , we split the row , writing everything in it except for the concerned column.
                else{
                    fw.write(e.split(":")[0]);
                    fw.write(":");
                    String[] Line = e.split(":")[2].split("#");
                    fw.write(Table.int_to_Str(Line.length - 1));
                    fw.write(":");
                    for(String f : Line)
                    {
                        if(!f.split("\\$")[0].equals(Name))
                        {
                            fw.write(f);
                            fw.write("#");
                        }
                    }
                    fw.write("\n");
                }
            }
            fw.close();

        return true;
            //deleting the column from each row in the dbt file
            //FileWriter fww = new FileWriter(table.cdb + "/" + table.Name + ".dbt");
        }catch(IOException ioe)
        {
            System.out.println("Error : An error occurred while dropping the column .");
            return false;
        }
    }
    public void SetPrimary()
    {
        isPrimary = true;
    }

    public static String ReadFile(String FileName) throws IOException
    {
        String out = "";
        try(FileReader fr = new FileReader(FileName))
        {
            int c;
            do{
                c = (int)fr.read();
                if(c > 0)
                {
                    out += (char)c;
                }
            }while(c > 0);
        }catch(IOException Ioe)
        {
            System.out.println("Failed to read file .");
            System.exit(0);
        }
        return out;
    }
}
