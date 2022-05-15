package aggregate;

import aggregate.mappers.KeyExtractorMapper;
import aggregate.reducers.AggregationReducer;
import aggregate.writables.AggregationKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class AggregationJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Path inputPath = new Path(strings[0]);
        Path outputPath = new Path(strings[1]);

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Main.class);

        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(KeyExtractorMapper.class);
        job.setMapOutputKeyClass(AggregationKey.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(AggregationReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);


        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}
