import java.io.FileReader;
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
    public int GetCol(Table table)throws IOException
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
    public void SetPrimary()
    {
        isPrimary = true;
    }
    private String ReadFile(String FileName) throws IOException
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
