import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TaskFTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[4];

        /*
        1. put the data.txt into a folder in your PC.
        2. add the path for the following two files.
            Windows: update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            Mac or Linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

//        input[0] = "file:///C:/Users/Kseniia/Documents/GitHub/CS4433_Project1/src/main/data/friends.csv";
//        input[1] = "file:///C:/Users/Kseniia/Documents/GitHub/CS4433_Project1/src/main/data/pages.csv";
//        input[2] = "file:///C:/Users/Kseniia/Documents/GitHub/CS4433_Project1/src/main/data/access_logs.csv";
//        input[3] = "file:///C:/Users/Kseniia/Documents/GitHub/CS4433_Project1/output";

        input[0] = "hdfs://localhost:9000/project1/friends.csv";
        input[1] = "hdfs://localhost:9000/project1/pages.csv";
        input[2] = "hdfs://localhost:9000/project1/access_logs.csv";
        input[3] = "hdfs://localhost:9000/project1/TaskF";

        TaskF taskF = new TaskF();
        boolean result = taskF.debug(input);

        assertTrue("The Hadoop job did not complete successfully", result);
    }

}