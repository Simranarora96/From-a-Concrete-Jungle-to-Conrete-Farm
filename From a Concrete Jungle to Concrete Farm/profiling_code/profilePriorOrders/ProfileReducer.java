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

			context.write(new Text("Count of Product Values = "), new IntWritable(count));
                        context.write(new Text("Minimum Product ID Value = "), new IntWritable(min_val));
                        context.write(new Text("Maximum Product ID Value = "), new IntWritable(max_val));
                        break;

                default :
        }
    }
}
