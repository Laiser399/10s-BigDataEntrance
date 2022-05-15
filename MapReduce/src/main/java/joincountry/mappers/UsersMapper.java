package joincountry.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import joincountry.enums.TextType;
import joincountry.writables.TypedText;

import java.io.IOException;

public class UsersMapper extends Mapper<Object, Text, LongWritable, TypedText> {
    private final Logger logger = Logger.getLogger(UsersMapper.class);
    private final LongWritable outputKey = new LongWritable();
    private final TypedText outputValue = new TypedText();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] userValues = value.toString().split("\u0001", -1);
        if (userValues.length < 2) {
            String errorMessage = String.format("Expected 2 values in user line. Got %d.", userValues.length);
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        Long userId = getUserId(userValues);
        if (userId == null) {
            logger.warn(String.format("Could not parse user id. Line: \"%s\".", value));
            return;
        }

        outputKey.set(userId);
        outputValue.setTextType(TextType.USER);
        outputValue.set(value);
        context.write(outputKey, outputValue);
    }

    private Long getUserId(String[] userValues) {
        try {
            return Long.parseLong(userValues[0]);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
