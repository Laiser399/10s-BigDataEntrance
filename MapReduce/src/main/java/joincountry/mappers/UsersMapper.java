package joincountry.mappers;

import joincountry.enums.RowType;
import joincountry.helpers.LongHelper;
import joincountry.writables.TypedRow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class UsersMapper extends Mapper<Object, Text, LongWritable, TypedRow> {
    private final Logger logger = Logger.getLogger(UsersMapper.class);
    private final LongWritable outputKey = new LongWritable();
    private final TypedRow outputValue = new TypedRow();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] userValues = value.toString().split("\u0001", -1);
        if (userValues.length < 2) {
            String errorMessage = String.format("Expected 2 values in user line. Got %d.", userValues.length);
            logger.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        Long userId = LongHelper.parseLongOrNull(userValues[0]);
        if (userId == null) {
            logger.warn(String.format("Could not parse user id. Line: \"%s\".", value));
            return;
        }

        outputKey.set(userId);
        outputValue.setRowType(RowType.USER);
        outputValue.setRow(value.toString());
        context.write(outputKey, outputValue);
    }
}
