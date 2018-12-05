import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;
import java.util.HashMap;


/**
 * This class is responsible for searching the index created
 * by a HadoopIndexer, as well as returning search results
 */
public class HadoopSearcher
{
    static HashMap<Text, DocumentWordPair[]> Index;
    static final String SearchTermKey = "SEARCH_TERMS";

    public static void main(String args[])
            throws IOException, InterruptedException, ClassNotFoundException
    {
        //readIndexFile(new Path(args[0]), new Configuration());

        Job j = configureSearchJob(args[0], new Path(args[1]), new Path(args[2]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // Overwrite the in-memory index with the contents of the
    // SequenceFile specified by the given path
    public static void readIndexFile(Path filePath, Configuration config)
        throws IOException
    {
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
        reader.close();
    }

    // Input terms must be comma-separated
    public static Job configureSearchJob(String inputTerms, Path indexPath, Path outputPath)
            throws IOException
    {
        Configuration c = new Configuration();
        c.set(SearchTermKey, inputTerms);
        Job job = Job.getInstance(c);
        job.setJarByClass(HadoopSearcher.class);
        job.setJobName("SearchDocuments");

        job.setMapperClass(SearchMap.class);
        job.setReducerClass(SearchCombine.class);
        //job.setReducerClass(SearchReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DocumentWordPair.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, indexPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }


    public static class SearchMap extends Mapper<Text, IndexEntry, Text, DocumentWordPair>
    {
        @Override
        public void map(Text term, IndexEntry entry, Context context)
                throws IOException, InterruptedException
        {
            String searchTerms = context.getConfiguration().get(SearchTermKey);
            String[] terms = searchTerms.split(" ");

            for(int i=0; i < terms.length; i++)
            {
                if (terms[i].compareToIgnoreCase(term.toString()) == 0)
                {
                    for (DocumentWordPair doc : (DocumentWordPair[]) entry.get())
                    {
                        context.write(doc.filePath, doc);
                    }
                }
            }
        }
    }

    // Compute a document's rank, and emit the rank-filename pair to the next phase
    public static class SearchCombine extends Reducer<Text, DocumentWordPair, LongWritable, Text>
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

            context.write(sum, fileName);
        }
    }

    // When the splits are sent from the combiner to the reducer, they will be sorted.
    // So, by the time this class executes its code, Hadoop will have already sorted
    // our output. We are okay to emit the results as we find them (if we only have
    // one reducer, that is).
    public static class SearchReduce extends Reducer<LongWritable, Text, LongWritable, Text>
    {
        @Override
        public void reduce(LongWritable rank, Iterable<Text> fileNames, Context context)
                throws IOException, InterruptedException
        {
            for (Text fileName : fileNames)
            {
                context.write(rank, fileName);
            }
        }
    }

}
