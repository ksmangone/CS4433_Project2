import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class dataGenerator {

    public static void main(String[] args) throws Exception{
        //Create data file with points

        int dataSize = 3000;
        int maxValueXY = 5000;

        writeDatasetToCSV(dataSize, maxValueXY, "src/main/data/dataset.csv");
    }

    public static void writeDatasetToCSV(int size, int maxValue, String filename) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            // Write CSV header
            writer.println("id,x,y");

            Random random = new Random();

            // Write data
            for (int i = 0; i < size; i++) {
                int x = random.nextInt(maxValue + 1);
                int y = random.nextInt(maxValue + 1);
                writer.println(i+ "," + x + "," + y);
            }
        } catch (IOException e) {
            System.out.println("CSV file failed.");
        }
    }

}
