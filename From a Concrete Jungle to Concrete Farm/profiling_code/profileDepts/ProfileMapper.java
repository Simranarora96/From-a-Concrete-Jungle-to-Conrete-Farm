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

        String [] order_details;
	int len;
                     
        order_details = value.toString().split(",");
        len = order_details[1].length();
   
        context.write(new Text("val_0"), new IntWritable(Integer.parseInt(order_details[0])));
        context.write(new Text("val_1"), new IntWritable(len));                                      	           	                	  	
   }
}
