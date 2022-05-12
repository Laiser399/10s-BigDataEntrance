package mapusers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static final String LOCATION_MAPPINGS_PATH = "SomeFile";

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new MapUsersJob(), args);

        System.exit(result);
    }
}
