import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String line;    
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
       
	 // (0) order_id, (1) product_id, (2) add_to_cart_order - removed, (3) reordered - removed
         context.write(NullWritable.get(), new Text(Integer.parseInt(order_details[0]) + "," + 
         Integer.parseInt(order_details[1])));
    }
}
