import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.OutputStreamWriter;

public class dataGenerator {

    public static void main(String[] args) throws Exception{
        //Create data file with points

        //Change to 3000
        int dataSize = 3000;
        int maxValueXY = 5000;

        writeDatasetToCSV(dataSize, maxValueXY, "src/main/data/dataset.csv");
    }

    public static void writeDatasetToCSV(int size, int maxValue, String filename) {
        Configuration conf = new Configuration();

        try (FileSystem fs = FileSystem.get(conf)) {
            Path hdfsPath = new Path(filename);
            FSDataOutputStream outputStream = fs.create(hdfsPath);

            PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream));

            // Write CSV header
//            writer.println("id,x,y");

            Random random = new Random();

            // Write data
            for (int i = 0; i < size; i++) {
                int x = random.nextInt(maxValue + 1);
                int y = random.nextInt(maxValue + 1);
                writer.println(x + "," + y);
            }

            writer.close();
            outputStream.close();

        } catch (IOException e) {
            System.out.println("CSV file failed.");
        }
    }

}
