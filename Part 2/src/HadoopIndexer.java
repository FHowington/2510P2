import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;
import java.util.*;

/**
 * This class is responsible for indexing documents in a Hadoop cluster
 */
public class HadoopIndexer
{
    public static void main(String args[])
            throws IOException, InterruptedException, ClassNotFoundException
    {
        Job j = configureIndexJob(new Path(args[0]), new Path(args[1]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // Run the map/reduce job(s) necessary for indexing the documents
    public static Job configureIndexJob(Path inputFolder, Path outputPath)
            throws IOException
    {
        Job job = Job.getInstance();
        job.setJarByClass(HadoopIndexer.class);
        job.setJobName("IndexDocuments");

        job.setMapperClass(IndexMap.class);
        job.setReducerClass(IndexReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DocumentWordPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IndexEntry.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, inputFolder);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }


    // This class is meant to map a collection of documents to nodes
    // which will produce a word-document-count key-value pair
    public static class IndexMap extends Mapper<LongWritable, Text, Text, DocumentWordPair>
    {
        @Override
        public void map(LongWritable documentId, Text documentText, Context context)
                throws IOException, InterruptedException
        {
            Text fileName = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
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
                context.write(currentWord, new DocumentWordPair(fileName, currentWord, count));
            }
        }
    }

    // This class is meant to reduce a collection of document-word-counts
    // into an index/list searchable by other Map/Reducers
    public static class IndexReduce extends Reducer<Text, DocumentWordPair, Text, IndexEntry>
    {
        @Override
        public void reduce(Text term, Iterable<DocumentWordPair> documentCounts, Context context)
                throws IOException, InterruptedException
        {
            List<DocumentWordPair> output = new LinkedList<>();
            for (DocumentWordPair count : documentCounts)
            {
                output.add(count);
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
                    return left.filePath.compareTo(right.filePath);
                }
            });

            context.write(term, new IndexEntry(output));
        }
    }
}
