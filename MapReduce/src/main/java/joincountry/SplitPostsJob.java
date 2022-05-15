package joincountry;

import joincountry.mappers.CommentsMapper;
import joincountry.mappers.PostsMapper;
import joincountry.mappers.UsersMapper;
import joincountry.reducers.JoinReducer;
import joincountry.writables.TypedText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class SplitPostsJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Path postsPath = new Path(strings[0]);
        Path commentsPath = new Path(strings[1]);
        Path usersPath = new Path(strings[2]);
        Path outputPath = new Path(strings[3]);

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, SplitPostsJob.class.getName());
        job.setJarByClass(Main.class);

        MultipleInputs.addInputPath(job, postsPath, SequenceFileInputFormat.class, PostsMapper.class);
        MultipleInputs.addInputPath(job, commentsPath, SequenceFileInputFormat.class, CommentsMapper.class);
        MultipleInputs.addInputPath(job, usersPath, SequenceFileInputFormat.class, UsersMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(TypedText.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        MultipleOutputs.addNamedOutput(
                job,
                Main.QUESTIONS_OUTPUT_NAME,
                TextOutputFormat.class,
                NullWritable.class,
                Text.class
        );
        MultipleOutputs.addNamedOutput(
                job,
                Main.ANSWERS_OUTPUT_NAME,
                TextOutputFormat.class,
                NullWritable.class,
                Text.class
        );
        MultipleOutputs.addNamedOutput(
                job,
                Main.COMMENTS_OUTPUT_NAME,
                TextOutputFormat.class,
                NullWritable.class,
                Text.class
        );

        job.setReducerClass(JoinReducer.class);
        job.setNumReduceTasks(1);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}
