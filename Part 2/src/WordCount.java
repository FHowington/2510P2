import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


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

    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        public Map2() {
        }

        // How do we map a mapping? Each line has a single word, and multiple files mapped to that word with an occurrence #
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            /*Get the name of the file using context.getInputSplit()method*/
            String line = value.toString();
            //Split the line in words
            String words[] = line.split("-");
            Text keyWord = new Text(words[0].replaceAll("\\s+",""));
            String[] vals = words[1].split(" ");
            for (String s : vals) {
                //for each word emit word as key and file name as value
                context.write(keyWord, new Text(s));
                System.out.println(s);
            }
        }
    }


    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        public Reduce2() {
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

    public static void main(String[] args) throws Exception {
        // Plan is this: Keep master inverted index in wordcount/index
        // When new file is loaded, delete whatever is in wordcount/output, put result into index? from normal map
        // then run map on


        if (args[2].equals("1")) {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "UseCase1");
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
            Path outputPath = new Path(args[1]);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, outputPath);
            //deleting the output path automatically from hdfs so that we don't have delete it explicitly
            FileSystem hdfs = FileSystem.get(conf);

            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } else if (args[2].equals("2")) {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "UseCase1");
            //Defining the output key and value class for the mapper
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setJarByClass(WordCount.class);
            FileSystem hdfs = FileSystem.get(conf);
            // Move output to same folder as current index
            hdfs.rename(new Path("wordcount/output/part-r-00000"), new Path("wordcount/index/tempindex"));
            job.setMapperClass(Map2.class);
            job.setReducerClass(Reduce2.class);
            //Defining the output value class for the mapper
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            Path outputPath = new Path("wordcount/tempresult");
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, outputPath);

            //deleting the output path automatically from hdfs so that we don't have delete it explicitly
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else {
            // Now, we need to delete everything in the index folder, move result stored in tempresult to this folder
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            hdfs.delete(new Path("wordcount/index/tempindex"), true);
            hdfs.delete(new Path("wordcount/index/index"), true);
            hdfs.rename(new Path("wordcount/tempresult/part-r-00000"), new Path("wordcount/index/index"));
        }
    }

}