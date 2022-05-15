package joincountry.reducers;

import joincountry.Main;
import joincountry.helpers.LongHelper;
import joincountry.writables.TypedText;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JoinReducer extends Reducer<LongWritable, TypedText, NullWritable, Text> {
    private final Logger logger = Logger.getLogger(JoinReducer.class);
    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<TypedText> values, Context context) throws IOException, InterruptedException {
        Map<Long, String> userToCountry = new HashMap<>();
        List<String> questions = new ArrayList<>();
        List<String> answers = new ArrayList<>();
        List<String> comments = new ArrayList<>();

        for (TypedText value : values) {
            switch (value.getTextType()) {
                case QUESTION:
                    questions.add(value.toString());
                    break;
                case ANSWER:
                    answers.add(value.toString());
                    break;
                case COMMENT:
                    comments.add(value.toString());
                    break;
                case USER:
                    parseUserAndAddEntry(value.toString(), userToCountry);
                    break;
            }
        }

        joinAndWritePosts(questions, userToCountry, Main.QUESTIONS_OUTPUT_NAME);
        joinAndWritePosts(answers, userToCountry, Main.ANSWERS_OUTPUT_NAME);
        joinAndWriteComments(comments, userToCountry);
    }

    private void joinAndWritePosts(Iterable<String> posts, Map<Long, String> userToCountry, String outputName) throws IOException, InterruptedException {
        for (String post : posts) {
            String[] postValues = post.split("\u0001", -1);
            if (postValues.length < 21) {
                String charCodes = post.chars()
                        .mapToObj(Integer::toString)
                        .collect(Collectors.joining(","));
                String message = String.format(
                        "Got post with %d (< 21) values.\n" +
                                "Line: \"%s\".\n" +
                                "Char codes: %s",
                        postValues.length, post, charCodes
                );
                logger.error(message);
                throw new IllegalStateException(message);
            }

            String id = postValues[0];
            String date = postValues[4];
            Long ownerUserId = LongHelper.parseLongOrNull(postValues[8]);
            if (ownerUserId == null) {
                logger.warn(String.format("Could not parse post owner user id. Line: \"%s\"", post));
                continue;
            }
            if (!userToCountry.containsKey(ownerUserId)) {
                logger.warn(String.format("Not found country for post owner user id %d.", ownerUserId));
                continue;
            }
            String country = userToCountry.get(ownerUserId);

            outputValue.set(String.join("\u0001", id, date, country));
            multipleOutputs.write(outputName, NullWritable.get(), outputValue);
        }
    }

    private void joinAndWriteComments(Iterable<String> comments, Map<Long, String> userToCountry) throws IOException, InterruptedException {
        for (String comment : comments) {
            String[] commentValues = comment.split("\u0001", -1);
            String commentId = commentValues[0];
            String creationDate = commentValues[3];
            Long userId = LongHelper.parseLongOrNull(commentValues[5]);
            if (userId == null) {
                logger.warn(String.format("Could not parse comment user id. Line: \"%s\"", comment));
                continue;
            }
            if (!userToCountry.containsKey(userId)) {
                logger.warn(String.format("Not found country for comment user id %d.", userId));
                continue;
            }
            String country = userToCountry.get(userId);

            outputValue.set(String.join("\u0001", commentId, creationDate, country));
            multipleOutputs.write(Main.COMMENTS_OUTPUT_NAME, NullWritable.get(), outputValue);
        }
    }


    private void parseUserAndAddEntry(String userLine, Map<Long, String> userToCountry) {
        String[] userParts = userLine.split("\u0001", -1);
        if (userParts.length != 2) {
            String charCodes = userLine.chars()
                    .mapToObj(Integer::toString)
                    .collect(Collectors.joining(","));
            String message = String.format(
                    "Expected 2 parts in user line. Got %d.\n" +
                            "Line: \"%s\".\n" +
                            "Char codes: %s",
                    userParts.length, userLine, charCodes
            );
            logger.error(message);
            throw new IllegalStateException(message);
        }

        Long userId = LongHelper.parseLongOrNull(userParts[0]);
        if (userId == null) {
            String message = String.format("Could not parse user id. Line: \"%s\".", userLine);
            logger.error(message);
            throw new IllegalStateException(message);
        }

        userToCountry.put(userId, userParts[1]);
    }
}
