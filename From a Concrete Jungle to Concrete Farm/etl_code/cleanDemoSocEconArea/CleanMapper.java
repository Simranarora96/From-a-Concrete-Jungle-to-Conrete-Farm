import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class CleanMapper
	extends Mapper<LongWritable, Text, Text, Text> {

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
		String ky = "";
		String val = "";
		boolean valid = true;
	
		for (int i = 0; i < columns.length; i+=3) {
			int index = Integer.parseInt(columns[i]) - 1;
			if (index >= fields.length) {
				valid = false;
				break;
			}
					
			String v = fields[index];
			if (v.length() == 0) {
				valid = false;
				break;
			}

			String type = columns[i + 2];
			if (type.equals("int")) {
				if (v.charAt(0) == '"') {
					v = v.substring(1, v.length() - 1);
				}	
				v = v.replace(",", "");
			}
			
			if (i == 0) {
				ky = v;
			}

			if (i == 3) {
				val = v;
			} else {
				val = val + "," + v;
			}
		}
			
		if (valid && ky.length() > 0) {
			context.write(new Text(ky), new Text(val));
		}
	}
}
