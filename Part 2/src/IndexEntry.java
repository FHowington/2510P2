import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class IndexEntry extends ArrayWritable
{
    public IndexEntry()
    {
        super(IndexEntry.class);
    }
}

/**
 * This class is a simple container to hold a term's "posting"
 * (using the verbiage of the given PowerPoint slides)
 */
class DocumentWordPair
{
    public DocumentWordPair(String path, Text word, LongWritable count)
    {
        this.filePath = path;
        this.word = word;
        this.count = count;
    }

    /** The document ID */
    public String filePath;
    /**
     * The term in the document being tracked by this pair.
     * This is likely unnecessary, considering how the mappers
     * and reducers keep track of the word as the key already
     */
    public Text word;
    /** The number of occurrences of the word in the document*/
    public LongWritable count;
}
