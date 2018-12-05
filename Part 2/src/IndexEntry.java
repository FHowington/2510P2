import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class IndexEntry extends ArrayWritable
{
    public IndexEntry()
    {
        this(new DocumentWordPair[0]);
    }
    public IndexEntry(DocumentWordPair[] pairs)
    {
        super(IndexEntry.class, pairs);
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

        this.set(values);
    }
}

/**
 * This class is a simple container to hold a term's "posting"
 * (using the verbiage of the given PowerPoint slides).
 *
 * To be used as a ValueType in a map/reduce operation, this
 * class must implement Writable. To be used as a KeyType,
 * it must implement WritableComparable, which implements Writable.
 */
class DocumentWordPair implements WritableComparable<DocumentWordPair>
{
    public DocumentWordPair(Text path, Text word, LongWritable count)
    {
        this.filePath = path;
        this.word = word;
        this.count = count;
    }
    // Empty constructor for serialization
    public DocumentWordPair()
    {
        filePath = new Text();
        word = new Text();
        count = new LongWritable();
    }

    /** The document URL */
    public Text filePath;
    /** The term in the document being tracked by this pair */
    public Text word;
    /** The number of occurrences of the word in the document */
    public LongWritable count;

    public boolean matchesDocumentAndWord(DocumentWordPair other)
    {
        if (other != null)
        {
            return filePath.equals(other.filePath) && word.equals(other.word);
        }
        return false;
    }

    // --- Writable interface methods ---
    @Override
    public void write(DataOutput output) throws IOException
    {
        filePath.write(output);
        word.write(output);
        count.write(output);
    }

    @Override
    public void readFields(DataInput input) throws IOException
    {
        filePath.readFields(input);
        word.readFields(input);
        count.readFields(input);
    }

    // --- WritableComparable interface methods/necessities ---
    @Override
    public int compareTo(DocumentWordPair o)
    {
        // Sort by filePath, then by word, then by count
        int comparisonResult = filePath.compareTo(o.filePath);
        if (comparisonResult == 0)
        {
            comparisonResult = word.compareTo(o.word);
            if (comparisonResult == 0)
            {
                comparisonResult = count.compareTo(o.count);
            }
        }

        return comparisonResult;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof DocumentWordPair)
        {
            return this.compareTo((DocumentWordPair) o) == 0;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(filePath, word, count);
    }
}
