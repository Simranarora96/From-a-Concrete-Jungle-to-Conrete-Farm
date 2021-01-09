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

		// Ignore header
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
			v = v.trim();
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
	
			String name = columns[i + 1];
			name = name.toLowerCase();
			
			// only select stores within the five boroughs
			if (name.equals("county")) {
				v = v.toLowerCase();
				if (!(v.equals("bronx") || v.equals("kings") || v.equals("new york") || v.equals("queens") || v.equals("richmond"))){
					valid = false;
					break;
				}
			}

			// only select stores whose primary purpose is the sale of food for consumption at home
			if (name.equals("establishmenttype")) {
				if (!(v.charAt(0) == 'A' || (v.length() > 1 && v.charAt(0) == 'J' && v.charAt(1) == 'A'))) {
					valid = false;
					break;
				}
			}

			// only select stores less than 5000
			if (name.equals("squarefootage")) {
				int sqft = Integer.parseInt(v);
				if (sqft <= 5000) {
					valid = false;
					break;	
				}	
			}

			// only select stores with longitude/latitude 
			if (name.equals("location")){
				int open = v.lastIndexOf('(');
				if (open < 0) {
					valid = false;
					break;
				}
				int close = v.indexOf(')', open);
				if (close < 0) {
					valid = false;
					break;
				}
				//System.out.println(v + " " + open + " " + close);
				String latLong = v.substring(open + 1, close).replace(" ", "");
				v = latLong;		
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
