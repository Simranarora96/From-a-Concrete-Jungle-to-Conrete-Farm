import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Profile {
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: Profile <input path> <output path> <data_type>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(Profile.class);
		job.setJobName("Profile");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setMapperClass(ProfileMapper.class);
		job.setReducerClass(ProfileReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Configuration conf = job.getConfiguration();
		conf.set("mapred.textoutputformat.separator", " ");


		String dataType = args[2].trim().toLowerCase();
		if (dataType.equals("soc")) {
			conf.set("delimiter", "\t");
			conf.set("columns", "2 GeoName string 3 GeoID string 4 Borough string 5 TotalHouseholds int 75 AvgHouseholdSize float 225 Pop25AndOver int 230 LessThan9 int 235 9to12NoDip int"); // soc
		} else if (dataType.equals("demo")) {
			conf.set("delimiter", "\t");
			conf.set("columns", "2 GeoName string 3 GeoID string 4 Borough string 5 TotalPopulation int 10 MalePop int 15 FemalePop int 20 Under5 int 25 5to9 int 30 10to14 int 35 15to19 int 40 20to24 int 45 25to29 int 50 30to34 int 55 35to39 int 60 40to44 int 65 45to49 int 70 50to54 int 75 55to59 int 80 60to64 int 85 65to69 int 90 70to74 int 95 75to79 int 100 80to84 int 105 85AndOver int"); // dem
		} else if (dataType.equals("econ")) {
			conf.set("delimiter", "\t");
			conf.set("columns", "2 GeoName string 3 GeoID string 4 Borough string 40 CivilianLabourForce int 45 CivilianLabourForceUnemployed int 565 PopWPovertyStatus int 570 PopBelowPeverty int 340 HouseholdsWithFoodstampsOrSNAP int"); // econ
		} else if (dataType.equals("area")) {
			conf.set("delimiter", ",");
			conf.set("columns", "6 GeoName string 5 GeoID string 3 Borough string 8 AreaSqFt float"); // area
		} else {
			System.err.println("Usage: Profile <input path> <output path> <data_type>. data_type must be one of (soc | demo | econ | area)");
			System.exit(-1);
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
