import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Profile {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: Instacart <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")

        Job job = new Job(conf,"Profile" );
        job.setJarByClass(Profile.class);

        job.setMapperClass(ProfileMapper.class);
        job.setReducerClass(ProfileReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
