package stubs;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

public class TestDriver {

	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {

		/*
		 * Sets up the mapper test.
		 */
		TestMapper mapper = new TestMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Sets up the reducer test.
		 */
		TestReducer reducer = new TestReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Sets up the mapper/reducer test.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	/*
	 * Tests the mapper.
	 */
	@Test
	public void testMapper() {

		
		 // For this test, the mapper's input will be: 1 25 45 45" 
		 
		mapDriver.withInput(new LongWritable(1), new Text("25 45 45"));
		mapDriver.withOutput(new Text("25"), new IntWritable(1));
		mapDriver.withOutput(new Text("45"), new IntWritable(1));
		mapDriver.withOutput(new Text("45"), new IntWritable(1));
		mapDriver.runTest();
	}

	
	 // Tests the reducers.
	 
	@Test
	public void testReducer() {

		
		 // For this test, the reducer's input will be : 25 1 45 2. 
		 //The expected output is: 45 45 2.
		 
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("45"), values);
		reduceDriver.withOutput(new Text("45"), new IntWritable(2));
		reduceDriver.runTest();
	}

	
	 // Tests whether the mapper and reducer work together.
	 
	@Test
	public void testMapReduce() {

		
		//  For this test, the mapper's input will be: 1 25 45 45 
		// The reducer's expected output : 25, 1 and 45, 2. 
		 
		mapReduceDriver.withInput(new LongWritable(1), new Text("25 45 45"));
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		mapReduceDriver.withOutput(new Text("25"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("45"), new IntWritable(2));
		mapReduceDriver.runTest();

		// fail("Please implement test.");
	}
}
