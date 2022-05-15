package aggregate.reducers;

import aggregate.writables.AggregationKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AggregationReducer extends Reducer<AggregationKey, LongWritable, NullWritable, Text> {
    private final Text outputValue = new Text();

    @Override
    protected void reduce(AggregationKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long countSum = 0;
        for (LongWritable entriesCount : values) {
            countSum += entriesCount.get();
        }

        outputValue.set(String.join(
                "\t",
                key.getCountry(),
                Integer.toString(key.getYear()),
                Integer.toString(key.getQuarter()),
                Long.toString(countSum)
        ));
        context.write(NullWritable.get(), outputValue);
    }
}
