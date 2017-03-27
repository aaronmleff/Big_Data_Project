package stubs;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ModifyFileMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text text = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();

				line = line.replaceAll("::", ",");
		
					text.set(line);
					context.write(null, text);
				}
			}
		
	

