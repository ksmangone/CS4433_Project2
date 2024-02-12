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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

// Identify people p1 that have declared someone as their friend p2 yet
// who have never accessed their respective friend p2’s Facebook page
// and return the PersonID and Name of the person p1. (This
// may indicate that they don’t care enough to find out any news about
// their friend -- at least not via Facebook).
public class TaskF {

    public static class Map extends Mapper<Object, Text, Text, Text>{

        private java.util.Map<String, String> friendsMap = new HashMap<>();
        private java.util.Map<String, String> pagesMap = new HashMap<>();
        private Text text = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // read the record from Friends.csv into the distributed cache
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
                if(!split[1].equals("PersonID")) {
                    friendsMap.put(split[1], split[2]);
                }
            }
            // close the stream
            IOUtils.closeStream(reader);


            // read the record from Pages.csv into the distributed cache
//            URI[] cacheFiles1 = context.getCacheFiles();
//            Path path1 = new Path(cacheFiles1[1]);
            Path path1 = files[1];
            // open the stream
            FileSystem fs1 = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis1 = fs1.open(path1);
            // wrap it into a BufferedReader object which is easy to read a record
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(fis1,
                    "UTF-8"));
            // read the record line by line
            String line1;
            while (StringUtils.isNotEmpty(line1 = reader1.readLine())) {
                String[] split = line1.split(",");
                if(!split[0].equals("PersonID")) {
                    pagesMap.put(split[0], split[1]);
                }
            }
            // close the stream
            IOUtils.closeStream(reader1);
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read each line of the large data set (AccessLog.csv)
            String[] fields = value.toString().split(",");
            if(friendsMap.containsKey(fields[1]) && friendsMap.get(fields[1]).equals(fields[2])) {
                friendsMap.remove(fields[1]);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(java.util.Map.Entry<String, String> set : friendsMap.entrySet()) {
                context.write(new Text(set.getKey()), new Text(pagesMap.get(set.getKey())));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskF");
        job.setJarByClass(TaskC.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // a file in local file system is being used here as an example
//        job.addCacheFile(new URI(args[0]));
//        job.addCacheFile(new URI(args[1]));

        // Configure the DistributedCache
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(args[1]).toUri(), job.getConfiguration());
        DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[3]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);
    }

    public boolean debug(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskF");
        job.setJarByClass(TaskC.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // a file in local file system is being used here as an example
//        job.addCacheFile(new URI(args[0]));
//        job.addCacheFile(new URI(args[1]));

        // Configure the DistributedCache
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(args[1]).toUri(), job.getConfiguration());
        DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);

        // Delete the output directory if it exists
        Path outputPath = new Path(args[3]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        return ret;
    }

}
