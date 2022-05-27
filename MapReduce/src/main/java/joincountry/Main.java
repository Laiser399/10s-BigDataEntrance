package joincountry;

import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static final String QUESTIONS_OUTPUT_NAME = "questions";
    public static final String ANSWERS_OUTPUT_NAME = "answers";
    public static final String COMMENTS_OUTPUT_NAME = "comments";

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new JoinCountryJob(), args);

        System.exit(result);
    }
}
