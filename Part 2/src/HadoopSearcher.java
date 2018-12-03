import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.HashMap;


/**
 * This class is responsible for searching the index created
 * by a HadoopIndexer, as well as returning search results
 */
public class HadoopSearcher
{
    static HashMap<Text, DocumentWordPair[]> Index;

    public static void main(String args[])
            throws IOException, InterruptedException, ClassNotFoundException
    {
        readIndexFile(new Path(args[0]), new Configuration());
    }

    // Overwrite the in-memory index with the contents of the
    // SequenceFile specified by the given path
    public static void readIndexFile(Path filePath, Configuration config)
        throws IOException
    {
        // TODO: This assumes we're using the serialization in IndexEntry, etc.
        // It's not working right now, so we'll need to use something else if we
        // want to test the searcher immediately
        Index = new HashMap<>();

        Text key = new Text();
        IndexEntry value = new IndexEntry(new DocumentWordPair[0]);

        SequenceFile.Reader.Option file = SequenceFile.Reader.file(filePath);
        SequenceFile.Reader reader = new SequenceFile.Reader(config, file);

        while(reader.next(key, value))
        {
            DocumentWordPair[] values = (DocumentWordPair[])value.get();
            Index.put(key, values);

            System.out.print("\n" + key.toString() + "\t");
            for (DocumentWordPair p : values)
            {
                System.out.print("{" + p.filePath + ":" + p.count.get() + "}, ");
            }
            System.out.println();
        }
    }

    // TODO: Assume the index is already filled out by some other code
    public static class SearchMap extends Mapper<LongWritable, Text, Text, DocumentWordPair>
    {
        @Override
        public void map(LongWritable termId, Text searchTerm, Context context)
                throws IOException, InterruptedException
        {
            /*
            * Assuming we pass a list of search terms to the mapper,
            * we will try to find all the documents that have the term
            * we're looking for.
            * To get a document's rank, we want to sum the counts of
            * all search terms. We will do this in the reduce step
            */

            DocumentWordPair[] matchingDocuments = Index.get(searchTerm);
            if (matchingDocuments != null)
            {
                for (DocumentWordPair doc : matchingDocuments)
                {
                    context.write(new Text(doc.filePath), doc);
                }
            }
        }
    }


    public static class SearchReduce extends Reducer<Text, DocumentWordPair, Text, LongWritable>
    {
        @Override
        public void reduce(Text fileName, Iterable<DocumentWordPair> termCounts, Context context)
                throws IOException, InterruptedException
        {
            LongWritable sum = new LongWritable(0);

            // Iterate over all matching terms for this document, and sum their counts
            for (DocumentWordPair term : termCounts)
            {
                sum.set(sum.get() + term.count.get());
            }

            // TODO: Now, we output a file path and its rank. Some other module will need to
            // sort and display these results, unless we turn this SearchReduce into
            // SearchCombine, and come up with another reduce that sorts this output somehow
            context.write(fileName, sum);
        }
    }
}
