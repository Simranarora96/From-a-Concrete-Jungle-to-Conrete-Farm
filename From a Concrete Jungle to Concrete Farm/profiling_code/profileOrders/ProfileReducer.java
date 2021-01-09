import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfileReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        int min_val = Integer.MAX_VALUE;
        int max_val = 0;
	int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        String key_val = key.toString();

        while (iterator.hasNext()) {

            int value = iterator.next().get();

            if (value < min_val) {

                min_val = value;
            }

            if (value > max_val) {

                max_val = value;
            }
	    count += 1;
        }

        switch (key_val) {

                case "val_0" :

			context.write(new Text("Count of Order IDs = "), new IntWritable(count));
                        context.write(new Text("Minimum Order ID Value = "), new IntWritable(min_val));
                        context.write(new Text("Maximum Order ID Value = "), new IntWritable(max_val));
                        break;

                case "val_1" :

			context.write(new Text("Count of User IDs = "), new IntWritable(count));
                        context.write(new Text("Minimum User ID Value = "), new IntWritable(min_val));
                        context.write(new Text("Maximum User ID Value = "), new IntWritable(max_val));
                        break;
	
		case "val_2" :

			context.write(new Text("Count of Days of the Week = "), new IntWritable(count));
			context.write(new Text("Minumum Day of Week Value = "), new IntWritable(min_val));
			context.write(new Text("Maximum Day of Week Value = "), new IntWritable(max_val));
			break;

		case "val_3" :

			context.write(new Text("Count of Hours of the Day = "), new IntWritable(count));
			context.write(new Text("Minimum Hour of Day Value = "), new IntWritable(min_val));
			context.write(new Text("Maximum Hour of Day Value = "), new IntWritable(max_val));
			break;

		case "val_4" :

			context.write(new Text("Count of Days Since Prior Order Entries = "), new IntWritable(count));
			context.write(new Text("Minimum Days Since Prior Order = "), new IntWritable(min_val));
			context.write(new Text("Maximum Days Since Prior Order = "), new IntWritable(max_val));					
			break;

                default :
        }
    }
}
