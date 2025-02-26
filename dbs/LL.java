package dbs;

public class LL{
    public String value;
    public LL next;
    
    public LL(String str)
    {
        value = str;
        next = null;
    }
    public void add_node(String str)
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
    public int size()
    {
        int i = 1;
        if(this.next != null)
        {
            LL curr = this.next ;
            while(curr != null)
            {
                curr = curr.next;
                i++;
            }
        }
        return i ;
    }
}
