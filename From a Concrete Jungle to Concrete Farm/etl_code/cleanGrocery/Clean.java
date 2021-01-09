import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Clean {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Clean <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(Clean.class);
		job.setJobName("Clean");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CleanMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Configuration conf = job.getConfiguration();
		conf.set("mapred.textoutputformat.separator", ",");
		conf.set("delimiter", "\t");
		conf.set("columns", "1 County string 4 EstablishmentType string 5 EntityName string 11 City string 12 State string 13 ZipCode string 14 SquareFootage string 15 location string");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
