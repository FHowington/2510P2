import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


/**
 * This class is responsible for searching the index created
 * by a HadoopIndexer, as well as returning search results
 */
public class HadoopSearcher
{
    // TODO: Use the SequenceFile input format to read the KVPs directly
    // (we'll need to use the same output format for the indexer)
    public static class SearchMap extends MapReduceBase
            implements Mapper<Text, List<DocumentWordPair>, Text, List<DocumentWordPair>>
    {
        @Override
        public void map(Text term,
                        List<DocumentWordPair> documentCounts,
                        OutputCollector<Text, List<DocumentWordPair>> outputCollector,
                        Reporter reporter) throws IOException
        {
            // TODO: Find the index file(s) that may have the counts
            // for the terms we want. Find the term-list mappings
            // for the things we're searching for.

            //if (searchTermList.contains(term)
            //  outputCollector.collect(term, documentCounts);
            // Is that sufficient? Or do we want to collect them differently?
        }
    }


    public static class SearchCombine extends MapReduceBase
            implements Reducer<Text, List<DocumentWordPair>, LongWritable, LongWritable>
    {
        @Override
        public void reduce(Text term,
                           Iterator<List<DocumentWordPair>> documentCounts,
                           OutputCollector<LongWritable, LongWritable> outputCollector,
                           Reporter reporter) throws IOException
        {
            // TODO: Maybe aggregate all a document's matching terms to get its "rank"
        }
    }


    public static class SearchReduce extends MapReduceBase
            implements Reducer<LongWritable, LongWritable, Text, Text>
    {
        @Override
        public void reduce(LongWritable longWritable,
                           Iterator<LongWritable> iterator,
                           OutputCollector<Text, Text> outputCollector,
                           Reporter reporter) throws IOException
        {
            // TODO: Need to produce an ordered list, or at least give the most suitable document path
            // But the DocumentWordPairs don't store the document path.
            // So do we have some other module telling us the ID-path mappings?
        }
    }
}
