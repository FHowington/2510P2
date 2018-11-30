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
    // TODO: Not sure what the input/output key/value types should be
    // since I don't know yet what input readers are available
    public static class SearchMap extends MapReduceBase
            implements Mapper<Text, List<DocumentWordPair>, Text, Text>
    {
        @Override
        public void map(Text term,
                        List<DocumentWordPair> documentCounts,
                        OutputCollector<Text, Text> outputCollector,
                        Reporter reporter) throws IOException
        {
            // TODO: Find the index file(s) that may have the counts
            // for the terms we want. Find the term-list mappings
            // for the things we're searching for.
            // Then, maybe use a combiner to aggregate all a document's
            // matching terms into a singular "rank" value
            // Then, reduce the ranks into an ordered list
        }
    }


    // TODO: Still don't know what the input types should be for this phase
    public static class SearchCombine extends MapReduceBase
            implements Reducer<Text, Text, LongWritable, LongWritable>
    {
        @Override
        public void reduce(Text text,
                           Iterator<Text> iterator,
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
        }
    }
}
