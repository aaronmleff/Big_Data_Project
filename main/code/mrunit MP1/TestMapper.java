package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text age = new Text();
	private final static IntWritable movieID = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
		
		String[] record = value.toString().split(",");

		if (Integer.parseInt(record[1]) > 0) {
			age.set(record[1]);
			context.write(age, movieID);
		} 
		}catch (ArrayIndexOutOfBoundsException e) {
			
		}
		}
	}
