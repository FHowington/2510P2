import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
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
public class HadoopSearcher {

    static final String SearchTermKey = "SEARCH_TERMS";

    public static void main(String args[])
            throws IOException, InterruptedException, ClassNotFoundException {
        Job j = configureSearchJob(args[0], new Path(args[1]), new Path(args[2]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // Input terms must be comma-separated
    public static Job configureSearchJob(String inputTerms, Path indexPath, Path outputPath)
            throws IOException {
        Configuration c = new Configuration();
        c.set(SearchTermKey, inputTerms);
        Job job = Job.getInstance(c);
        job.setJarByClass(HadoopSearcher.class);
        job.setJobName("SearchDocuments");

        job.setMapperClass(SearchMap.class);
        job.setReducerClass(SearchReduce.class);
        //job.setReducerClass(SearchReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job, indexPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }


    public static class SearchMap extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String searchTerms = context.getConfiguration().get(SearchTermKey);
            String[] terms = searchTerms.split(" ");

            String line = value.toString();
            //Split the line in words
            String words[] = line.split("-");
            Text term = new Text(words[0].replaceAll("\\s+", ""));
            String[] vals = words[1].split(" ");


            for (int i = 0; i < terms.length; i++) {
                if (terms[i].compareToIgnoreCase(term.toString()) == 0) {
                    for (String s : vals) {
                        String[] delim = s.split("=");
                        int val = Integer.parseInt(delim[1]);
                        /*Check if file name is present in the HashMap ,if File name is not present then add the Filename to the HashMap and increment the counter by one , This condition will be satisfied on first occurrence of that word*/

                        //for each word emit word as key and file name as value
                        context.write(new Text(delim[0]), new LongWritable(val));
                    }
                }
            }
        }
    }


    // Compute a document's rank, and emit the rank-filename pair to the next phase
    public static class SearchReduce extends Reducer<Text, LongWritable, LongWritable, Text> {
        @Override
        public void reduce(Text fileName, Iterable<LongWritable> docCounts, Context context)
                throws IOException, InterruptedException {
            LongWritable sum = new LongWritable(0);

            // Iterate over all matching terms for this document, and sum their counts
            for (LongWritable val : docCounts) {
                sum.set(sum.get() + val.get());
            }

            context.write(sum, fileName);
        }
    }



}
