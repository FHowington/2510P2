import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * This class is responsible for indexing documents in a Hadoop cluster
 */
public class HadoopIndexer
{
    // This class is meant to map a collection of documents to nodes
    // which will produce a word-document-count key-value pair
    public static class IndexMap extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, DocumentWordPair>
    {
        @Override
        public void map(LongWritable documentId,
                        Text documentText,
                        OutputCollector<Text, DocumentWordPair> outputCollector,
                        Reporter reporter) throws IOException
        {
            // It is easier to count words in a document now, when
            // we have the documentId at our disposal, instead of
            // trying to aggregate them in a collect or reduce method
            HashMap<String, Long> wordCounts = new HashMap<>();
            StringTokenizer tokenizer = new StringTokenizer(documentText.toString());
            while (tokenizer.hasMoreTokens())
            {
                String token = tokenizer.nextToken();
                if (wordCounts.containsKey(token))
                {
                    wordCounts.put(token, wordCounts.get(token)+1);
                }
                else
                {
                    wordCounts.put(token, (long)1);
                }

            }

            // After counting all words in the document, send the key-value
            // aggregates to the reduce step
            for(String word : wordCounts.keySet())
            {
                Text currentWord = new Text(word);
                LongWritable count = new LongWritable(wordCounts.get(word));
                outputCollector.collect(currentWord, new DocumentWordPair(documentId, currentWord, count));
            }
        }
    }

    // This class is meant to reduce a collection of document-word-counts
    // into an index/list searchable by other Map/Reducers
    public static class IndexReduce extends MapReduceBase
            implements Reducer<Text, DocumentWordPair, Text, List<DocumentWordPair>>
    {
        @Override
        public void reduce(Text term,
                           Iterator<DocumentWordPair> documentCounts,
                           OutputCollector<Text, List<DocumentWordPair>> outputCollector,
                           Reporter reporter) throws IOException
        {
            List<DocumentWordPair> output = new LinkedList<>();
            while (documentCounts.hasNext())
            {
                output.add(documentCounts.next());
            }

            // Before writing the list of counts to a file,
            // we need to sort them
            output.sort(new Comparator<DocumentWordPair>(){
                @Override
                public int compare(DocumentWordPair left, DocumentWordPair right) {
                    // Sort first by word count
                    if (left.count.get() > right.count.get())
                        return -1;
                    if (left.count.get() < right.count.get())
                        return 1;

                    // Then by document ID in the event of a tie
                    if (left.id.get() > right.id.get())
                        return -1;
                    if (left.id.get() < right.id.get())
                        return 1;

                    return 0;
                }
            });

            outputCollector.collect(term, output);
        }
    }
}

/**
 * This class is a simple container to hold a term's "posting"
 * (using the verbiage of the given PowerPoint slides)
 */
class DocumentWordPair
{
    public DocumentWordPair(LongWritable id, Text word, LongWritable count)
    {
        this.id = id;
        this.word = word;
        this.count = count;
    }

    /** The document ID */
    public LongWritable id;
    /**
     * The term in the document being tracked by this pair.
     * This is likely unnecessary, considering how the mappers
     * and reducers keep track of the word as the key already
     */
    public Text word;
    /** The number of occurrences of the word in the document*/
    public LongWritable count;
}
