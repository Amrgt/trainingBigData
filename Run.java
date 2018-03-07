package ch.epfl.data.bigdata.hw2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Run {

	/**
	 * First map job take the text and extract a pair from the first number and
	 * another that it is sending as the key with the other numbers of the line
	 * as the value
	 */
	public static class Map1 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] list = (value.toString().split("\\s+,|\\s+|,\\s+|,"));

			for (int i = 1; i < list.length; i++) {
				String outKey = "";
				String outVal = "";

				// always the smallest first => regroup by key
				if (Integer.parseInt(list[0]) > Integer.parseInt(list[i])) {
					outKey = outKey.concat(list[i] + "," + list[0]);
				} else {
					outKey = outKey.concat(list[0] + "," + list[i]);
				}

				for (int j = 1; j < list.length; j++) {
					if (j != i) {
						outVal = outVal.concat(list[j]);
						outVal = outVal.concat(",");
					}
				}

				output.collect(new Text(outKey), new Text(outVal));
			}
		}
	}

	/**
	 * First reduce job receives a pair of numbers as the key with list of int
	 * as value; output each sorted triple in lists with a one representing an
	 * occurrence
	 */
	public static class Reduce1 extends MapReduceBase implements
			Reducer<Text, Text, IntWritable, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			Set<String> all = new HashSet<String>();
			int index = 0;

			while (values.hasNext()) {
				String[] list = values.next().toString().split(",");

				for (int i = 0; i < list.length; i++) {
					if (index != 0) {
						if (all.contains(list[i])) {
							String out = getTriple(key.toString(), list[i]);
							if (out != "") {
								output.collect(new IntWritable(1),
										new Text(out));
							}
						}
					}
					all.add(list[i]);
					++index;
				}
			}
		}
	}

	// utility function for Reduce1
	public static String getTriple(String key, String val) {
		String out = "";
		if (key != null && val != null) {
			String[] all = key.split(",");
			if (all.length == 2) {
				if (Integer.parseInt(all[1]) > Integer.parseInt(val)) {
					if (Integer.parseInt(all[0]) > Integer.parseInt(val)) {
						out = out.concat(val + "," + all[0] + "," + all[1]);
					} else {
						out = out.concat(all[0] + "," + val + "," + all[1]);
					}
				} else {
					out = out.concat(all[0] + "," + all[1] + "," + val);
				}
			}
		}
		return out;
	}

	/**
	 * Second map job returns key one with the value composed of all the triples
	 * possible such that the second reducer can count them
	 */
	public static class Map2 extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text triples,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			output.collect(new IntWritable(1), triples);
		}
	}

	/**
	 * Second reduce job increment a counter for each different triple
	 */
	public static class Reduce2 extends MapReduceBase implements
			Reducer<IntWritable, Text, Text, IntWritable> {
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			Set<String> vals = new HashSet<String>();

			while (values.hasNext()) {
				String string = values.next().toString();
				if (!vals.contains(string)) {
					vals.add(string);
				}
			}

			output.collect(new Text(""), new IntWritable(vals.size()));
		}
	}

	public static void main(String[] args) throws Exception {

		String input1 = args[0];
		String output2 = args[1];
		String input2 = output2 + "-tmp";
		String output1 = input2;

		JobConf job1 = new JobConf(Run.class);
		job1.setJobName("1st job");

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);

		job1.setInputFormat(TextInputFormat.class);
		job1.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path(input1));
		FileOutputFormat.setOutputPath(job1, new Path(output1));

		job1.setNumMapTasks(80);
		job1.setNumReduceTasks(80);
		JobClient.runJob(job1);

		JobConf job2 = new JobConf(Run.class);
		job2.setJobName("2nd job");

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		job2.setInputFormat(TextInputFormat.class);
		job2.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job2, new Path(input2));
		FileOutputFormat.setOutputPath(job2, new Path(output2));

		job2.setNumMapTasks(80);
		job2.setNumReduceTasks(80);
		JobClient.runJob(job2);
	}
}