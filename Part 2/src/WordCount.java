import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

public class WordCount {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public Map() {
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            /*Get the name of the file using context.getInputSplit()method*/
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            String line = value.toString();
            //Split the line in words
            String words[] = line.split(" ");
            for (String s : words) {
                //for each word emit word as key and file name as value
                context.write(new Text(s), new Text(fileName));
            }
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public Reduce() {
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            /*Declare the Hash Map to store File name as key to compute and store number of times the filename is occurred for as value*/
            HashMap<String, Integer> m = new HashMap<>();
            int count;
            for (Text t : values) {
                String str = t.toString();
                /*Check if file name is present in the HashMap ,if File name is not present then add the Filename to the HashMap and increment the counter by one , This condition will be satisfied on first occurrence of that word*/
                if (m.get(str) != null) {
                    count = m.get(str);
                    m.put(str, ++count);
                } else {
                    /*Else part will execute if file name is already added then just increase the count for that file name which is stored as key in the hash map*/
                    m.put(str, 1);
                }
            }
            /* Emit word and [file1->count of the word1 in file1 , file2->count of the word1 in file2... ] as output*/

            StringBuilder result = new StringBuilder();
            result.append("-");
            for (java.util.Map.Entry<String, Integer> entry : m.entrySet()) {
                result.append(entry.getKey()).append("=");
                result.append(entry.getValue());
                result.append(" ");
            }
            context.write(key, new Text(result.toString()));
        }
    }

    public static class MapIndex extends Mapper<LongWritable, Text, Text, Text> {
        public MapIndex() {
        }

        // How do we map a mapping? Each line has a single word, and multiple files mapped to that word with an occurrence #
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            /*Get the name of the file using context.getInputSplit()method*/
            String line = value.toString();
            //Split the line in words
            String words[] = line.split("-");
            Text keyWord = new Text(words[0].replaceAll("\\s+", ""));
            String[] vals = words[1].split(" ");
            for (String s : vals) {
                //for each word emit word as key and file name as value
                context.write(keyWord, new Text(s));
                System.out.println(s);
            }
        }
    }


    public static class ReduceIndex extends Reducer<Text, Text, Text, Text> {
        public ReduceIndex() {
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            /*Declare the Hash Map to store File name as key to compute and store number of times the filename is occurred for as value*/
            HashMap<String, Integer> m = new HashMap<>();
            int count;
            for (Text t : values) {
                // Values is now list of things like "FILE1=10"

                String str = t.toString();
                String[] delim = str.split("=");
                int val = Integer.parseInt(delim[1]);
                /*Check if file name is present in the HashMap ,if File name is not present then add the Filename to the HashMap and increment the counter by one , This condition will be satisfied on first occurrence of that word*/
                if (m.get(delim[0]) != null) {
                    count = m.get(delim[0]);
                    m.put(delim[0], count + val);
                } else {
                    /*Else part will execute if file name is already added then just increase the count for that file name which is stored as key in the hash map*/
                    m.put(delim[0], val);
                }
            }
            /* Emit word and [file1->count of the word1 in file1 , file2->count of the word1 in file2... ] as output*/
            StringBuilder result = new StringBuilder();
            result.append("-");
            for (java.util.Map.Entry<String, Integer> entry : m.entrySet()) {
                result.append(entry.getKey()).append("=");
                result.append(entry.getValue());
                result.append(" ");
            }
            context.write(key, new Text(result.toString()));
        }
    }

    private static synchronized boolean mapNew(String newFile) {
        Configuration conf = new Configuration();
        Job job;
        try {
            job = new Job(conf, "UseCase1");
            //Defining the output key and value class for the mapper
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setJarByClass(WordCount.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            //Defining the output value class for the mapper
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            Path outputPath = new Path("wordcount/output");
            FileInputFormat.addInputPath(job, new Path(newFile));
            FileOutputFormat.setOutputPath(job, outputPath);
            //deleting the output path automatically from hdfs so that we don't have delete it explicitly
            FileSystem hdfs = FileSystem.get(conf);


            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }

            return job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private static synchronized boolean mergeIndex() {
        Configuration conf = new Configuration();
        Job job;
        try {
            job = new Job(conf, "UseCase2");
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setJarByClass(WordCount.class);
            FileSystem hdfs = FileSystem.get(conf);
            // Move output to same folder as current index
            hdfs.rename(new Path("wordcount/output/part-r-00000"), new Path("wordcount/index/tempindex"));
            job.setMapperClass(MapIndex.class);
            job.setReducerClass(ReduceIndex.class);
            //Defining the output value class for the mapper
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            Path outputPath = new Path("wordcount/tempresult");
            FileInputFormat.addInputPath(job, new Path("wordcount/index"));
            FileOutputFormat.setOutputPath(job, outputPath);
            //deleting the output path automatically from hdfs so that we don't have delete it explicitly
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            return job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
        //Defining the output key and value class for the mapper
    }

    private static synchronized void cleanup() {
        // Now, we need to delete everything in the index folder, move result stored in tempresult to this folder
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            hdfs.delete(new Path("wordcount/index/tempindex"), true);
            hdfs.delete(new Path("wordcount/index/index"), true);
            hdfs.rename(new Path("wordcount/tempresult/part-r-00000"), new Path("wordcount/index/index"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static synchronized void main(String[] args) {
        // Plan is this: Keep master inverted index in wordcount/index
        // When new file is loaded, delete whatever is in wordcount/output, put result into index? from normal map
        // then run map on

        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.println("Enter next command, or help for list of commands");
            switch (sc.nextLine().toLowerCase()) {
                case "help":
                    System.out.println("Usage:\n" +
                            "Index- Specify location of file on HDFS to index\n" +
                            "Search- Search for terms within all indexed files\n" +
                            "Q - quit");
                    break;

                case "index":
                    System.out.println("Enter location of file to index on HDFS");
                    if (mapNew(sc.nextLine())) {
                        if (mergeIndex()) {
                            cleanup();
                            System.out.println("Success");
                        }
                    }
                    break;

                case "q":
                    return;
            }
        }
    }

}
