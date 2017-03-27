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

public class ModifyFile extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		/*
		 * Instantiate a Job object for your job's configuration.
		 */
		Job job = new Job(getConf());

	
		job.setJarByClass(ModifyFile.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("ModifyFile");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ModifyFileMapper.class);
		// setting the number of reducers to 0
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {

		
		int exitCode = ToolRunner.run(new ModifyFile(), args);
		System.exit(exitCode);
		
		if (args.length != 2) {
			System.out
					.printf("Usage: modifyFile <input dir> <output dir>\n");
			System.exit(-1);
		}
	}

}
