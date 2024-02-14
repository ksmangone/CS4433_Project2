import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class kMeansClustering {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        //Add setup to read file in memory

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            if(!values[0].equals("id")) {
                //Get point coordinates
                String pointX = values[1];
                String pointY = values[2];

                //Go through each centroid



            }
        }
    }

    //Pass K number/ input and output is fixed
    public static void main(String[] args) throws Exception{
        //Get number of centroids
        Integer k = Integer.valueOf(args[0]);
        //Create data file with centroids
        //Upload to HDFS
        dataGenerator.writeDatasetToCSV(k, 10000, "src/main/data/centroids.csv");

        //Start map-reduce job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "kMeans");
        job.setJarByClass(kMeansClustering.class);
        job.setMapperClass(kMeansClustering.Map.class);

    }

}
