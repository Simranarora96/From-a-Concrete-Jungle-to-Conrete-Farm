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
	int index = 0;
	String temp  = "";

            order_details = value.toString().split(",");

	    if (order_details[1].startsWith("\"")) {

		for (int i = 0; i < order_details.length; i++) {

			if (order_details[i].endsWith("\"")) {
			
				index = i;
			}
		}

		for (int i = 1; i <= index; i++) {

			temp += order_details[i]; 	
		}

		len = temp.length();
		context.write(new Text("val_0"), new IntWritable(Integer.parseInt(order_details[0])));
		context.write(new Text("val_1"), new IntWritable(len));
		context.write(new Text("val_2"), new IntWritable(Integer.parseInt(order_details[order_details.length - 1])));

        } else {

		len = order_details[1].length();
		context.write(new Text("val_0"), new IntWritable(Integer.parseInt(order_details[0])));
		context.write(new Text("val_1"), new IntWritable(len));
		context.write(new Text("val_2"), new IntWritable(Integer.parseInt(order_details[2])));;
         }
}
}
