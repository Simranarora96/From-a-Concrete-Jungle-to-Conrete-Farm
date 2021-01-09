import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Clean {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: Instacart <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")

        Job job = new Job(conf,"Clean" );
        job.setJarByClass(Clean.class);

        job.setMapperClass(CleanMapper.class);
        // job.setReducerClass(InstacartCleanReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
} 
