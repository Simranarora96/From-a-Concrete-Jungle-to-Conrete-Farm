import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

public class ProfileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        // String line;    
        String [] order_details;
	order_details = value.toString().split(",");
       
        // (0) order_id, (1) product_id, (2) add_to_cart_order - removed, (3) reordered - removed
        context.write(new Text("val_0"), new IntWritable(Integer.parseInt(order_details[0]))); // order_id
        context.write(new Text("val_1"), new IntWritable(Integer.parseInt(order_details[1]))); // product_id
    }
}
