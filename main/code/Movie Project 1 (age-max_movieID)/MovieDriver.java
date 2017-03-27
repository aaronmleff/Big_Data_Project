package stubs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		
		//Creates job, also specifies the jar file containing mapper, reducer, driver.
		Job job = new Job(getConf());
		job.setJarByClass(MovieDriver.class);

		job.setJobName("leff7766: age groups that watch the most movies");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
		job.setMapperClass(MovieMapper.class);
		job.setReducerClass(MovieReducer.class);
		
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {

		/*
		 * The MapReduce job starts, and we wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		int exitCode = ToolRunner.run(new MovieRunner(), args);
		System.exit(exitCode);
		/*
		 * Validates that two arguments were indeed passed from the command line.
		 */
		if (args.length != 2) {
			System.out
					.printf("Usage: MovieDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
	}
}
