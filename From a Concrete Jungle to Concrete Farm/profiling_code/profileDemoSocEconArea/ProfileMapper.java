import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class ProfileMapper
	extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		if (key.get() == 0) {
			return;
		}

		Configuration conf = context.getConfiguration();
		String param = conf.get("columns");
		String []columns = param.split(" ");

		String line = value.toString();
		String []fields = line.split(conf.get("delimiter"));
	
		for (int i = 0; i < columns.length; i+=3) {
			int index = Integer.parseInt(columns[i]) - 1;
			if (index >= fields.length) {
				continue;
			}
			String val = fields[index];
			context.write(new LongWritable(i), new Text(val));
		}

	}
}
