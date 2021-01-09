import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String [] order_details;

        // Remove Header
        if (key.get() == 0) {

            return;

        } else {

            order_details = value.toString().split(",");
        }

        // Check for Null values
        for (int i = 0; i < order_details.length; i++) {

            if (order_details[i] == null) {

                return;
            }
        }

        // Remove "eval_set" column, convert days_since_prior order to int, add 0 for first orders in days_since_prior order
        // (0) order_id, (1) user_id,(2) eval_set, (3) order_number, (4) order_dow,(5) order_hour_of_day, (6) days_since_prior_order

        if (order_details.length == 7) { 

            context.write(NullWritable.get(), new Text(order_details[0] + "," + order_details[1] + "," +
            order_details[4] + "," + (Integer.parseInt(order_details[5])) + "," + 
            (int)(Double.parseDouble(order_details[6])))); 

        } else if (order_details.length == 6 && order_details[3] == "1") {

            context.write(NullWritable.get(), new Text(order_details[0] + "," + order_details[1] + "," +
            order_details[4] + "," + order_details[5] + ",0"));
        }
    }
}
