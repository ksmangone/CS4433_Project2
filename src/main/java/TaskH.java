import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

// Report all people with a Facebook who are more ‘popular’ than the
// average person on the Facebook site, namely, those who have more
// friendship relationships than the average number of friend
// relationships per person across all people in the site.
public class TaskH {

    public static class Map extends Mapper<Object, Text, Text, NullWritable>{

        private int total = 0;
        private int num = 0;

        private java.util.Map<String, Integer> friendsMap = new HashMap<>();

        // read the record from Friends.csv into the distributed cache

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
                if(friendsMap.containsKey(split[2])) {
                    total++;
                    friendsMap.put(split[2], friendsMap.get(split[2])+1);
                } else if(!split[1].equals("PersonID")) {
                    total++;
                    friendsMap.put(split[2], 1);
                }
            }
            // close the stream
            IOUtils.closeStream(reader);
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read each line of the data set (Friends.csv)
            String[] fields = value.toString().split(",");
            if(!friendsMap.containsKey(fields[0]) && !fields[0].equals("PersonID")) {
                friendsMap.put(fields[0], 0);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            num = friendsMap.size();
            double average = total / num;
            for(java.util.Map.Entry<String, Integer> set : friendsMap.entrySet()) {
                if(set.getValue() > average) {
                    context.write(new Text(set.getKey()), NullWritable.get());
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskH");
        job.setJarByClass(TaskH.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // a file in local file system is being used here as an example
//        job.addCacheFile(new URI(args[0]));

        // Configure the DistributedCache
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[2]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);
    }

    public boolean debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskH");
        job.setJarByClass(TaskH.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // a file in local file system is being used here as an example
//        job.addCacheFile(new URI(args[0]));

        // Configure the DistributedCache
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[2]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        return ret;
    }

}
