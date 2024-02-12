import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskE2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] accessInfo = value.toString().split(",");
            String byWho = accessInfo[1];
            String whatPage = accessInfo[2];
            if(!byWho.contains("ByWho")) {
                context.write(new Text(byWho + ", " + whatPage), one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private Map<String, IntWritable> accessPairs = new HashMap<>();
        private Map<String, IntWritable> distinct = new HashMap<>();
        private Map<String, IntWritable> total = new HashMap<>();
        private IntWritable resultPairs = new IntWritable();
        private HashMap visited = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int distinct = 0;
            int sum = 0;
            String[] keyString = key.toString().split(",");
            String byWho = keyString[0];
            for (IntWritable val : values) {
                sum += val.get();
                if (!accessPairs.containsKey(key)) {
                    distinct += val.get();
                    accessPairs.put(key.toString(), new IntWritable(distinct));
                }
                total.put(key.toString(), new IntWritable(sum));
            }
//            resultPairs.set(distinct);
//            context.write(new Text(byWho), resultPairs);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<String, IntWritable> itr : accessPairs.entrySet()) {
                // account number
                String key = itr.getKey().toString().split(",")[0];
                // handle the total number of accesses
                if (!total.containsKey(key)) {
                    total.put(key, itr.getValue());
                } else {
                    IntWritable currentVal = total.get(key);
                    IntWritable addVal = itr.getValue();
                    IntWritable newVal = new IntWritable(currentVal.get() + addVal.get());
                    total.replace(key, newVal);
                }
                // handle the number of distinct accesses
                if (!distinct.containsKey(key)) {
                    distinct.put(key, itr.getValue());
                } else {
                    IntWritable currentVal = distinct.get(key);
                    IntWritable addVal = new IntWritable(1);
                    IntWritable newVal = new IntWritable(currentVal.get() + addVal.get());
                    distinct.replace(key, newVal);
                }
            }
            for(Map.Entry<String, IntWritable> itr : accessPairs.entrySet()){
                String key = itr.getKey().toString().split(",")[0];
                if(!visited.containsKey(key)){
                    context.write(new Text("Person ID: " + key + ", Total Accesses: " + total.get(key) + ", # Distinct Accesses: "), distinct.get(key));
                    visited.put(key, 1);
                }
            }
        }
    }

    public boolean debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskE2");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        return ret;
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskE2");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);
    }
}