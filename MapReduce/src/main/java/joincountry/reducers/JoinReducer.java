package joincountry.reducers;

import joincountry.Main;
import joincountry.enums.RowType;
import joincountry.writables.TypedRow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class JoinReducer extends Reducer<LongWritable, TypedRow, NullWritable, Text> {
    private final Logger logger = Logger.getLogger(JoinReducer.class);
    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<TypedRow> values, Context context) throws IOException, InterruptedException {
        List<String> questions = new ArrayList<>();
        List<String> answers = new ArrayList<>();
        List<String> comments = new ArrayList<>();

        String country = null;

        Iterator<TypedRow> iterator = values.iterator();
        while (iterator.hasNext()) {
            TypedRow value = iterator.next();
            RowType rowType = value.getRowType();
            String row = value.getRow();

            if (rowType == RowType.USER) {
                country = getCountryFromUserRow(row);
                break;
            }

            switch (rowType) {
                case QUESTION:
                    questions.add(row);
                    break;
                case ANSWER:
                    answers.add(row);
                    break;
                case COMMENT:
                    comments.add(row);
                    break;
                default:
                    throw new IllegalStateException(String.format("Got unexpected row type: %s", rowType));
            }
        }

        if (country == null) {
            logger.warn(String.format("No user with id %d", key.get()));
            return;
        }

        for (String question : questions) {
            validateAndWritePost(question, country, Main.QUESTIONS_OUTPUT_NAME);
        }
        for (String answer : answers) {
            validateAndWritePost(answer, country, Main.ANSWERS_OUTPUT_NAME);
        }
        for (String comment : comments) {
            validateAndWriteComment(comment, country);
        }

        while (iterator.hasNext()) {
            TypedRow typedRow = iterator.next();
            RowType rowType = typedRow.getRowType();
            String row = typedRow.getRow();

            switch (rowType) {
                case QUESTION:
                    validateAndWritePost(row, country, Main.QUESTIONS_OUTPUT_NAME);
                    break;
                case ANSWER:
                    validateAndWritePost(row, country, Main.ANSWERS_OUTPUT_NAME);
                    break;
                case COMMENT:
                    validateAndWriteComment(row, country);
                    break;
                case USER:
                    throw new IllegalStateException(String.format("Got second user on reduce step. User id: %d.", key.get()));
            }
        }
    }

    private String getCountryFromUserRow(String user) {
        String[] userParts = user.split("\u0001", -1);
        if (userParts.length != 2) {
            String charCodes = user.chars()
                    .mapToObj(Integer::toString)
                    .collect(Collectors.joining(","));
            String message = String.format(
                    "Expected 2 parts in user line. Got %d.\n" +
                            "Line: \"%s\".\n" +
                            "Char codes: %s",
                    userParts.length, user, charCodes
            );
            logger.error(message);
            throw new IllegalStateException(message);
        }

        return userParts[1];
    }

    private void validateAndWritePost(String post, String country, String outputName) throws IOException, InterruptedException {
        String[] postValues = post.split("\u0001", -1);
        if (postValues.length < 21) {
            throwAndLogInvalidValuesInRowCount(post, 21, postValues.length);
        }

        String id = postValues[0];
        String date = postValues[4];

        outputValue.set(String.join("\u0001", id, date, country));
        multipleOutputs.write(outputName, NullWritable.get(), outputValue);
    }

    private void validateAndWriteComment(String comment, String country) throws IOException, InterruptedException {
        String[] commentValues = comment.split("\u0001", -1);
        if (commentValues.length < 6) {
            throwAndLogInvalidValuesInRowCount(comment, 6, commentValues.length);
        }

        String commentId = commentValues[0];
        String creationDate = commentValues[3];


        outputValue.set(String.join("\u0001", commentId, creationDate, country));
        multipleOutputs.write(Main.COMMENTS_OUTPUT_NAME, NullWritable.get(), outputValue);
    }

    private void throwAndLogInvalidValuesInRowCount(String row, int expectedValuesCount, int actualValuesCount) {
        String charCodes = row.chars()
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(","));
        String message = String.format(
                "Got row with %d values. Expected %d.\n" +
                        "Line: \"%s\".\n" +
                        "Char codes: %s",
                actualValuesCount, expectedValuesCount, row, charCodes
        );
        logger.error(message);
        throw new IllegalStateException(message);
    }
}
