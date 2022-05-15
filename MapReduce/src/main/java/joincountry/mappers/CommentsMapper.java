package joincountry.mappers;

import joincountry.enums.TextType;
import joincountry.helpers.LongHelper;
import joincountry.writables.TypedText;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class CommentsMapper extends Mapper<Object, Text, LongWritable, TypedText> {
    private final Logger logger = Logger.getLogger(CommentsMapper.class);
    private final LongWritable outputKey = new LongWritable();
    private final TypedText outputValue = new TypedText();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] commentValues = value.toString().split("\u0001", -1);
        if (commentValues.length < 6) {
            String errorMessage = String.format("Expected 6 values in comment line. Got %d.", commentValues.length);
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        Long userId = LongHelper.parseLongOrNull(commentValues[5]);
        if (userId == null) {
            if (commentValues[5].length() == 0) {
                logger.warn(String.format(
                        "Comment's user id is empty.\n" +
                                "Line: \"%s\"",
                        value
                ));
                return;
            }

            String message = String.format(
                    "Could not parse user id in comment.\n" +
                            "Line: \"%s\".\n" +
                            "Value: \"%s\".",
                    value, commentValues[5]);
            logger.warn(message);
            throw new IllegalStateException(message);
        }
        if (userId == -1) {
            logger.warn(String.format("Got comment with user id equal -1. Comment id: %s.", commentValues[0]));
            return;
        }

        outputKey.set(userId);
        outputValue.setTextType(TextType.COMMENT);
        outputValue.set(value);
        context.write(outputKey, outputValue);
    }
}
