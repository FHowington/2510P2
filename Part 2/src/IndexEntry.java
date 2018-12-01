import org.apache.hadoop.io.ArrayWritable;

public class IndexEntry extends ArrayWritable
{
    public IndexEntry()
    {
        super(IndexEntry.class);
    }
}
