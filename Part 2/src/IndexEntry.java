import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class IndexEntry extends ArrayWritable
{
    public IndexEntry(DocumentWordPair[] pairs)
    {
        super(IndexEntry.class, pairs);
    }

    public IndexEntry(List<DocumentWordPair> pairs)
    {
        this((DocumentWordPair[]) pairs.toArray());
    }

    @Override
    public Class getValueClass() {
        return DocumentWordPair.class;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Writable[] values = get();

        out.writeInt(values.length);
        for (Writable v : values)
        {
            v.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        DocumentWordPair[] values = new DocumentWordPair[length];

        for (int i=0; i < length; i++)
        {
            values[i] = new DocumentWordPair();
            values[i].readFields(in);
        }
    }
}

/**
 * This class is a simple container to hold a term's "posting"
 * (using the verbiage of the given PowerPoint slides)
 */
class DocumentWordPair implements Writable
{
    public DocumentWordPair(String path, Text word, LongWritable count)
    {
        this.filePath = path;
        this.word = word;
        this.count = count;
    }
    // Empty constructor for serialization
    public DocumentWordPair() {}

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


    @Override
    public void write(DataOutput output) throws IOException
    {
        output.writeBytes(filePath);
        output.writeBytes(word.toString());
        output.writeLong(count.get());
    }

    @Override
    public void readFields(DataInput input) throws IOException
    {
        filePath = WritableUtils.readString(input);
        word = new Text(WritableUtils.readString(input));
        count = new LongWritable(input.readLong());
    }
}
