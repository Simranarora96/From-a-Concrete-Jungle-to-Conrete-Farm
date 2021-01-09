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

        String [] order_details;
	int len;
	int index = 0;
	String temp  = "";

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

	    if (order_details[1].startsWith("\"")) {

		for (int i = 0; i < order_details.length; i++) {

			if (order_details[i].endsWith("\"")) {
			
				index = i;
			}
		}

		for (int i = 1; i <= index; i++) {

			temp += order_details[i]; 	
		}
            }	

		if (order_details[1].startsWith("\"")) {

	        	context.write(NullWritable.get(), new Text(order_details[0] + "," + temp +
	        	"," + (Integer.parseInt(order_details[order_details.length - 1]))));

                } else {

	        	context.write(NullWritable.get(), new Text(order_details[0] + "," + 
	        	order_details[1] + "," + (Integer.parseInt(order_details[3]))));
  		}
    }
}
