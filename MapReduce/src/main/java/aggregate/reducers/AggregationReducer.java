package aggregate.reducers;

import aggregate.writables.AggregationKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AggregationReducer extends Reducer<AggregationKey, LongWritable, AggregationKey, LongWritable> {
    private final LongWritable outputValue = new LongWritable();

    @Override
    protected void reduce(AggregationKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long countSum = 0;
        for (LongWritable entriesCount : values) {
            countSum += entriesCount.get();
        }
        outputValue.set(countSum);
        context.write(key, outputValue);
    }
}
