package joincountry.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import joincountry.enums.TextType;
import joincountry.writables.TypedText;

import java.io.IOException;

public class PostsMapper extends Mapper<Object, Text, LongWritable, TypedText> {
    private final Logger logger = Logger.getLogger(PostsMapper.class);
    private final LongWritable outputKey = new LongWritable();
    private final TypedText outputValue = new TypedText();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] postValues = value.toString().split("\u0001", -1);
        if (postValues.length < 21) {
            String errorMessage = String.format("Expected 21 values in line. Got %d.", postValues.length);
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        Long ownerUserId = getOwnerUserId(postValues);
        if (ownerUserId == null) {
            logger.warn(String.format("Could not parse post owner user id. Line: \"%s\".", value));
            return;
        }

        TextType textType = getTextType(postValues);
        if (textType == null) {
            logger.warn(String.format("Could not parse post type. Line: \"%s\".", value));
            return;
        }

        outputKey.set(ownerUserId);
        outputValue.setTextType(textType);
        outputValue.set(value);
        context.write(outputKey, outputValue);
    }

    private Long getOwnerUserId(String[] postValues) {
        try {
            return Long.parseLong(postValues[8]);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private TextType getTextType(String[] postValues) {
        try {
            int postTypeId = Integer.parseInt(postValues[1]);
            switch (postTypeId) {
                case 1:
                    return TextType.QUESTION;
                case 2:
                    return TextType.ANSWER;
                default:
                    return null;
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
