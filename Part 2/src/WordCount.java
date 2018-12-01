import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCount {

//    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//
//        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
//            String line = value.toString();
//            StringTokenizer tokenizer = new StringTokenizer(line);
//            while (tokenizer.hasMoreTokens()) {
//                word.set(tokenizer.nextToken());
//                output.collect(word, one);
//            }
//        }
//    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public Map(){}
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            /*Get the name of the file using context.getInputSplit()method*/
            String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
//Split the line in words
            String words[] = line.split(" ");
            for (String s : words) {
//for each word emit word as key and file name as value
                context.write(new Text(s), new Text(fileName));
            }
        }
    }

//    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
//        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
//            int sum = 0;
//            while (values.hasNext()) {
//                sum += values.next().get();
//            }
//            output.collect(key, new IntWritable(sum));
//        }
//    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public Reduce(){}
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            /*Declare the Hash Map to store File name as key to compute and store number of times the filename is occurred for as value*/
            HashMap m = new HashMap();
            int count = 0;
            for (Text t : values) {
                String str = t.toString();
                /*Check if file name is present in the HashMap ,if File name is not present then add the Filename to the HashMap and increment the counter by one , This condition will be satisfied on first occurrence of that word*/
                if (m != null && m.get(str) != null) {
                    count = (int) m.get(str);
                    m.put(str, ++count);
                } else {
                    /*Else part will execute if file name is already added then just increase the count for that file name which is stored as key in the hash map*/
                    m.put(str, 1);
                }
            }
            /* Emit word and [file1→count of the word1 in file1 , file2→count of the word1 in file2 ………] as output*/
            context.write(key, new Text(m.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
//        JobConf conf = new JobConf(WordCount.class);
//        conf.setJobName("wordcount");
//
//        conf.setOutputKeyClass(Text.class);
//        conf.setOutputValueClass(IntWritable.class);
//
//        conf.setMapperClass(Map.class);
//        conf.setCombinerClass(Reduce.class);
//        conf.setReducerClass(Reduce.class);
//        conf.setInputFormat(TextInputFormat.class);
//        conf.setOutputFormat(TextOutputFormat.class);
//        FileInputFormat.setInputPaths(conf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//        JobClient.runJob(conf);

        Configuration conf2 = new Configuration();
        Job job = new Job(conf2, "UseCase1");
        //Defining the output key and value class for the mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        //Defining the output value class for the mapper
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        Path outputPath = new Path(args[1]);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outputPath);
        //deleting the output path automatically from hdfs so that we don't have delete it explicitly
        outputPath.getFileSystem(conf2).delete(outputPath);
        //exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}