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

// Determine which people have favorites. That is, for each Facebook
// page owner, determine how many total accesses to Facebook pages they
// have made (as reported in the AccessLog) and how many distinct
// Facebook pages they have accessed in total.
public class TaskE {

    public static class Map extends Mapper<Object, Text, Text, Text>{

        private java.util.Map<String, String> pagesMap = new HashMap<>();
        private java.util.Map<String, Integer> uniqueMap = new HashMap<>();

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
                    pagesMap.put(split[0], "0,0");
                }
            }
            // close the stream
            IOUtils.closeStream(reader);
        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read each line of the data set (AccessLogs.csv)
            String[] fields = value.toString().split(",");
            if(!fields[0].equals("AccessID")) {
                String[] vals = pagesMap.get(fields[1]).split(",");
                int total = Integer.parseInt(vals[0]);
                int unique = Integer.parseInt(vals[1]);
                pagesMap.put(fields[1], (total+1) + "," + unique);

                if(!uniqueMap.containsKey(fields[1] + "," + fields[2])) {
                    uniqueMap.put(fields[1] + "," + fields[2], 1);
                }

            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(java.util.Map.Entry<String, Integer> set : uniqueMap.entrySet()) {
                String page = set.getKey().split(",")[0];
                String[] vals = pagesMap.get(page).split(",");
                pagesMap.put(page, vals[0] + "," + (Integer.parseInt(vals[1]) + 1));
            }

            for(java.util.Map.Entry<String, String> set : pagesMap.entrySet()) {
                context.write(new Text(set.getKey()), new Text(set.getValue()));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskE");
        job.setJarByClass(TaskE.class);
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
        Job job = Job.getInstance(conf, "TaskE");
        job.setJarByClass(TaskE.class);
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
