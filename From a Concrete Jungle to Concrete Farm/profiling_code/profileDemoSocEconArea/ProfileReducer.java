import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.text.NumberFormat;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfileReducer
	extends Reducer<LongWritable, Text, Text, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();	
		String []columns = conf.get("columns").split(" ");

		int index = (int)key.get();
		String type = columns[index + 2];
		String profile = type;
		int count = 0;
		int invalid = 0;

		switch(type) {
			case "string":
				Map<String, Integer> freq = new HashMap<String, Integer>();
				for (Text value : values) {
					String s = value.toString();
					if (s.length() == 0) {
						invalid += 1;
						count += 1;
						continue;
					}
					if (freq.containsKey(s)) {
						freq.put(s, freq.get(s) + 1);
					} else {
						freq.put(s, 1);
					}	
					count += 1;
				}
				profile = profile + " count:" + count + " countInvalid:" + invalid + " countDistinct:" + freq.size();
				break;
			case "float":
				float maxF = Float.MIN_VALUE;
				float minF = Float.MAX_VALUE;
				for (Text value : values) {
					String s = value.toString();
					if (s.length() == 0) {
						invalid += 1;
						count += 1;
						continue;
					}
					if (s.charAt(0) == '"') {
						s = s.substring(1, s.length() - 1);
					}

					float n = Float.parseFloat(s);
					if (n > maxF) {
						maxF = n;
					}

					if (n < minF) {
						minF = n;
					}
					count += 1;
				}
				profile = profile + " count:" + count + " countInvalid:" + invalid + " min:" + minF + " max:" + maxF;
				break;
			case "int":
				int maxI = Integer.MIN_VALUE;
				int minI = Integer.MAX_VALUE;
				for (Text value : values) {
					String s = value.toString();
					if (s.length() == 0) {
						invalid += 1;
						count += 1;
						continue;
					}
					if (s.charAt(0) == '"') {
						s = s.substring(1, s.length() - 1);
					}

					try {
						int n = NumberFormat.getNumberInstance(Locale.US).parse(s).intValue();
						if (n > maxI) {
							maxI = n;
						}

						if (n < minI) {
							minI = n;
						}
					} catch (Exception e) {
						System.out.println("Could not parse.");
					}
					count += 1;
				}
				profile = profile + " count:" + count + " countInvalid:" + invalid + " min:" + minI + " max:" + maxI;
				break;
			default:
				System.out.println("Unkown type " + type);
		}

 		context.write(new Text(columns[index + 1]), new Text(profile));
	}
}
