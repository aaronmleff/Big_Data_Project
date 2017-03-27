package stubs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** movie mapper 
 * 
 */
public class MovieMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	static enum Movie {
		Total_Movies, BAD_RECORD
	};

	Text age = new Text();
	private final static IntWritable movieID = new IntWritable(1);
	
	IntWritable intwritable = new IntWritable();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
		//this examines the relevant info that I used in the SQL statement to
		//combine the data sets (see paper), such as userID and age
						
			String[] record = value.toString().split(",");
			
		    if (Integer.parseInt(record[5]) > 0){
		    	age.set(record[1]);
		    	context.write(age, movieID);
		    }                                           		
			context.getCounter(Movie.Total_Movies).increment(1);		
		}  catch (ArrayIndexOutOfBoundsException e2) {
			context.getCounter(Movie.BAD_RECORD).increment(1);
		}  catch (NumberFormatException e3) {		 
		}
	}
}

