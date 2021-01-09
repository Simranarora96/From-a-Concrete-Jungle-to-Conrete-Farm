import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

public class ProfileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //Text text_1 = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        // String line;    
        String [] order_details;
	order_details = value.toString().split(",");

            // (0) order_id, (1) user_id,(2) order_dow,(3) order_hour_of_day, (4) days_since_prior_order
	         context.write(new Text("val_0"), new IntWritable(Integer.parseInt(order_details[0]))); // order_id
                 context.write(new Text("val_1"), new IntWritable(Integer.parseInt(order_details[1]))); // user_id
                 context.write(new Text("val_2"), new IntWritable(Integer.parseInt(order_details[2]))); // order_dow
                 context.write(new Text("val_3"), new IntWritable(Integer.parseInt(order_details[3]))); // order_hour_of_day
	         context.write(new Text("val_4"), new IntWritable(Integer.parseInt(order_details[4]))); // days_since_prior_order
    }
}
