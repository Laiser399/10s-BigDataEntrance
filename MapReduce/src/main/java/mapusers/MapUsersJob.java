package mapusers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

public class MapUsersJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Path usersPath = new Path(strings[0]);
        String locationMappingsPath = strings[1];
        Path outputPath = new Path(strings[2]);

        Configuration configuration = new Configuration();
        configuration.set(Main.LOCATION_MAPPINGS_PATH, locationMappingsPath);

        Job job = Job.getInstance(configuration, MapUsersJob.class.getName());
        job.setJarByClass(Main.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(UsersMapper.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, usersPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}
