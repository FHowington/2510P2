import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * This class is responsible for merging index files together
 */
public class IndexMerger
{
    // Since each map/reduce task could execute in its own JVM,
    // we cannot rely on the tasks accessing shared memory.
    // Each mapper needs to read the index file for itself
    static final String NewIndexPathKey = "NEW_INDEX_PATH";

    public static void main(String args[])
            throws IOException, InterruptedException, ClassNotFoundException
    {
        Job j = configureMergeJob(args[0], new Path(args[1]), new Path(args[2]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);

        // TODO: Delete old index files, maybe
    }

    public static Job configureMergeJob(String newIndexFilePath, Path existingIndexPath, Path mergedIndexPath)
            throws IOException
    {
        Configuration c = new Configuration();
        c.set(NewIndexPathKey, newIndexFilePath);

        Job job = Job.getInstance(c);
        job.setJarByClass(IndexMerger.class);
        job.setJobName("MergeIndex");

        job.setMapperClass(MergeMap.class);
        job.setReducerClass(HadoopIndexer.IndexReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DocumentWordPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IndexEntry.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, existingIndexPath);
        FileOutputFormat.setOutputPath(job, mergedIndexPath);

        return job;
    }

    // Read the contents of the specified index file, and return the mapping
    public static HashMap<Text, DocumentWordPair[]> readIndexFile(Path filePath, Configuration config, boolean verbose)
            throws IOException
    {
        HashMap<Text, DocumentWordPair[]> index = new HashMap<>();

        Text key = new Text();
        IndexEntry value = new IndexEntry(new DocumentWordPair[0]);

        SequenceFile.Reader.Option file = SequenceFile.Reader.file(filePath);
        SequenceFile.Reader reader = new SequenceFile.Reader(config, file);

        while(reader.next(key, value))
        {
            DocumentWordPair[] values = (DocumentWordPair[])value.get();
            index.put(key, values);

            if (verbose)
            {
                System.out.print("\n" + key.toString() + "\t");
                for (DocumentWordPair p : values) {
                    System.out.print("{" + p.filePath + ":" + p.count.get() + "}, ");
                }
                System.out.println();
            }
        }
        reader.close();

        return index;
    }


    // The existing IndexReduce step expects Term-Pair mappings
    public static class MergeMap extends Mapper<Text, IndexEntry, Text, DocumentWordPair>
    {
        static HashMap<Text, DocumentWordPair[]> Index;

        @Override
        public void map(Text term, IndexEntry entry, Context context)
                throws IOException, InterruptedException
        {
            if (Index == null)
            {
                Configuration c = context.getConfiguration();
                Index = readIndexFile(new Path(c.get(NewIndexPathKey)), c, false);
            }

            List<DocumentWordPair> existingPairs = Arrays.asList((DocumentWordPair[]) entry.get());
            DocumentWordPair[] otherPairs = Index.get(term);
            if (otherPairs != null)
            {
                // If the index to be merged has entries for this term,
                // see if any of those entries overwrite existing ones
                // for files we know about
                for (DocumentWordPair pair : otherPairs)
                {
                    // Whether this pair is new or overwrites an existing one,
                    // it should be sent to the reduce step
                    context.write(term, pair);

                    // However, if this pair overwrites an existing one, we
                    // should not send the old value to the reducer
                    int existingSize = existingPairs.size();
                    for (int i=0; i < existingSize; i++)
                    {
                        if (pair.matchesDocumentAndWord(existingPairs.get(i)))
                        {
                            existingPairs.remove(i);
                            break;
                            // There should only have been one match.
                            // TODO: Does this assumption hold?
                        }
                    }

                    // TODO: Remove duplicates
                }
            }

            // Whatever existing entries remain (after duplicates were removed),
            // pass them to the reducer
            for (DocumentWordPair existing : existingPairs)
            {
                context.write(term, existing);
            }
        }

    }
}
