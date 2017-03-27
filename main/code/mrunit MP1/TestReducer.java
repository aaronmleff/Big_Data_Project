package stubs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable intWritable = new IntWritable();
	Text text = new Text();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for (IntWritable value : values) {
			count=count+ value.get();
		}

		intWritable.set(count);
		context.write(key, intWritable);
	}
}