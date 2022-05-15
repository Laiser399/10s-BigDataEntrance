package aggregate;

import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new AggregationJob(), args);

        System.exit(result);
    }
}
