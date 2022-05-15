package aggregate.mappers;

import aggregate.writables.AggregationKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.IsoFields;

public class KeyExtractorMapper extends Mapper<Object, Text, AggregationKey, LongWritable> {
    private final AggregationKey outputKey = new AggregationKey();
    private final LongWritable outputValue = new LongWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\u0001", -1);
        if (parts.length != 3) {
            String message = String.format(
                    "Expected 3 values in row. Got %d.\n" +
                            "Line: \"%s\".",
                    parts.length, value
            );
            throw new IllegalStateException(message);
        }

        LocalDateTime dateTime;
        try {
            dateTime = LocalDateTime.parse(parts[1], DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        } catch (DateTimeParseException e) {
            String message = String.format(
                    "Could not parse date time.\n" +
                            "Value: \"%s\".\n" +
                            "Line: \"%s\".",
                    parts[1], value
            );
            throw new RuntimeException(message);
        }

        outputKey.setCountry(parts[2]);
        outputKey.setYear(dateTime.getYear());
        outputKey.setQuarter(dateTime.get(IsoFields.QUARTER_OF_YEAR));
        context.write(outputKey, outputValue);
    }
}
