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

// For each Facebook page owner p2, compute the “connectedness factor”
// of p2. That is, for each person p2 in your dataset, report the
// owner p2’s name, and the number of people p1 that are listing p2
// as their friend. For page owners that aren't listed as anybody's
// friend, i.e., they are not in the Friends file, return a count of
// zero.
public class TaskD {

    public static class Map extends Mapper<Object, Text, Text, Text>{

        private java.util.Map<String, String> pagesMap = new HashMap<>();

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
                if(!split[0].equals("PersonID")) {
                    pagesMap.put(split[0], 0 + "," + split[1]);
                }
            }
            // close the stream
            IOUtils.closeStream(reader);
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read each line of the data set (Friends.csv)
            String[] fields = value.toString().split(",");
            if(!fields[0].equals("FriendRel")) {
                String p2 = fields[2];
                String[] vals = pagesMap.get(p2).split(",");
                String name = vals[1];
                int num = Integer.parseInt(vals[0]);
                pagesMap.put(p2, (num+1) + "," + name);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(java.util.Map.Entry<String, String> set : pagesMap.entrySet()) {
                String[] vals = set.getValue().split(",");
                String name = vals[1];
                String num = vals[0];
                context.write(new Text(name), new Text(num));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
