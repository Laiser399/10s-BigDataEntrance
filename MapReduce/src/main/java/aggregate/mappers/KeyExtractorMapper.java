package aggregate.mappers;

import aggregate.writables.AggregationKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyExtractorMapper extends Mapper<Object, Text, AggregationKey, LongWritable> {
}
