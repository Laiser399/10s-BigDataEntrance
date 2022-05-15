package aggregate.reducers;

import aggregate.writables.AggregationKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregationReducer extends Reducer<AggregationKey, LongWritable, AggregationKey, LongWritable> {
}
