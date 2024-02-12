import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;

// Find the top 10 popular Facebook pages, namely, those that got the
// most accesses based on your AccessLog dataset compared to all other
// pages. Return their Id, Name and Nationality.
public class TaskB2 {

    public static class AccessLogsMap extends Mapper<Object, Text, Text, IntWritable> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if(!fields[2].contains("WhatPage")) {
                context.write(new Text(fields[2]), new IntWritable(1));
            }
        }

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private PriorityQueue<SumTextPair> top10 = new PriorityQueue<>();

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
            int sum = 0;
            for(IntWritable x: values) {
                sum += x.get();
            }

            top10.add(new SumTextPair(sum, key.toString()));

        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(int i = 0; i < 10; i++) {
                SumTextPair pair = top10.poll();
                context.write(new Text(pair.getText()), new IntWritable(pair.getSum()));
            }
        }

        private static class SumTextPair implements Comparable<SumTextPair> {
            private final int sum;
            private final String text;

            public SumTextPair(int sum, String text) {
                this.sum = sum;
                this.text = text;
            }

            public int getSum() {
                return sum;
            }

            public String getText() {
                return text;
            }

            @Override
            public int compareTo(SumTextPair other) {
                return Integer.compare(other.sum, this.sum);
            }
        }

    }

    public static class ReplicatedJoinPagesMap extends Mapper<Object, Text, Text, NullWritable> {

        private Map<String, String> pageMap = new HashMap<>();
        private Text text = new Text();

        // read the record from Pages.csv into the distributed cache

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            URI[] cacheFiles = context.getCacheFiles();
//            Path path = new Path(cacheFiles[0]);
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            Path path = files[0];
            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            // wrap it into a BufferedReader object which is easy to read a record
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis,
                    "UTF-8"));
            // read the record line by line
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] split = line.split(",");
                pageMap.put(split[0], split[0] + ',' + split[1] + ',' + split[2]);
            }
            // close the stream
            IOUtils.closeStream(reader);
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read each line of the large data set (AccessLog.csv)
            String[] fields = value.toString().split("\t");
            // use the WhatPage (ID) pulled from the AccessLog.csv data set
            // to retrieve Page id, name, and nationality from the lookup table in memory
            String page = pageMap.get(fields[0]);
            text.set(page);
            // output the mapper key-value pair
            if(!text.toString().contains("WhatPage")) {
                context.write(text, NullWritable.get());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AccessLogsJob");
        job.setJarByClass(TaskB2.class);
        job.setMapperClass(AccessLogsMap.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[3]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);



        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "JoinPagesJob");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(ReplicatedJoinPagesMap.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        // a file in local file system is being used here as an example
//        job2.addCacheFile(new URI(args[0]));

        // Configure the DistributedCache
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job2.getConfiguration());
        DistributedCache.setLocalFiles(job2.getConfiguration(), args[0]);

        // Delete the output directory if it exists
        Path outputPath2 = new Path(args[2]);
        FileSystem fs2 = outputPath2.getFileSystem(conf2);
        if (fs2.exists(outputPath2)) {
            fs2.delete(outputPath2, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, outputPath2);

        boolean ret = job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);
    }

    public boolean debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AccessLogsJob");
        job.setJarByClass(TaskB2.class);
        job.setMapperClass(AccessLogsMap.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[3]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);



        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "JoinPagesJob");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(ReplicatedJoinPagesMap.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        // a file in local file system is being used here as an example
//        job2.addCacheFile(new URI(args[0]));

        // Configure the DistributedCache
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job2.getConfiguration());
        DistributedCache.setLocalFiles(job2.getConfiguration(), args[0]);

        // Delete the output directory if it exists
        Path outputPath2 = new Path(args[2]);
        FileSystem fs2 = outputPath2.getFileSystem(conf2);
        if (fs2.exists(outputPath2)) {
            fs2.delete(outputPath2, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, outputPath2);

        boolean ret = job2.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        return ret;
    }

}
