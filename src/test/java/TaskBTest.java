import static org.junit.Assert.*;
import org.junit.Test;

public class TaskBTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        /*
        1. put the data.txt into a folder in your PC.
        2. add the path for the following two files.
            Windows: update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            Mac or Linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

//        input[0] = "file:///HostPath/project1_data/pages.csv";
//        input[1] = "file:///HostPath/project1_data/access_logs.csv";
//        input[2] = "file:///Users/Kseniia/IdeaProjects/Project1/output";

        input[0] = "hdfs://localhost:9000/project1/pages.csv";
        input[1] = "hdfs://localhost:9000/project1/access_logs.csv";
        input[2] = "hdfs://localhost:9000/project1/TaskB";

        TaskB taskB = new TaskB();
        boolean result = taskB.debug(input);

        assertTrue("The Hadoop job did not complete successfully", result);




        String[] input2 = new String[4];

//        input2[0] = "file:///HostPath/project1_data/pages.csv";
//        input2[1] = "file:///HostPath/project1_data/access_logs.csv";
//        input2[2] = "file:///Users/Kseniia/IdeaProjects/Project1/output";
//        input2[3] = "file:///Users/Kseniia/IdeaProjects/Project1/exchange";

        input2[0] = "hdfs://localhost:9000/project1/pages.csv";
        input2[1] = "hdfs://localhost:9000/project1/access_logs.csv";
        input2[2] = "hdfs://localhost:9000/project1/TaskB2";
        input2[3] = "hdfs://localhost:9000/project1/TaskB2_exchange";

        TaskB2 taskB2 = new TaskB2();
        boolean result2 = taskB2.debug(input2);

        assertTrue("The Hadoop job did not complete successfully", result2);
    }

}