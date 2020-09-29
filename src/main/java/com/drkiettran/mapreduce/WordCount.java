package com.drkiettran.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Let's see if we could wordcount to work. This is a classic program that is
 * used for concept of mapreduce programming.
 * 
 * Added comments.
 * <p>
 * https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 */
public class WordCount {

	private static final String APP_NAME = "Word Count";
	private static final String PART_R_00000 = "/part-r-00000";
	private static Job job;
	private static Path outputPath;
	private static Configuration conf;
	private static Path outputFile;

	public WordCount() throws IOException {
	}

	public int run(Job job) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		String classPath = "/opt/hadoop/hadoop/etc/hadoop," + "/opt/hadoop/hadoop/share/hadoop/common/*,"
				+ "/opt/hadoop/hadoop/share/hadoop/common/lib/*," + "/opt/hadoop/hadoop/share/hadoop/hdfs/*,"
				+ "/opt/hadoop/hadoop/share/hadoop/hdfs/lib/*," + "/opt/hadoop/hadoop/share/hadoop/mapreduce/*,"
				+ "/opt/hadoop/hadoop/share/hadoop/mapreduce/lib/*," + "/opt/hadoop/hadoop/share/hadoop/yarn/*,"
				+ "/opt/hadoop/hadoop/share/hadoop/yarn/lib/*";

		Configuration conf = job.getConfiguration();
		conf.set("yarn.application.classpath", classPath);
		conf.set("yarn.app.mapreduce.am.env", "/opt/hadoop/hadoop");
		conf.set("mapreduce.map.env", "/opt/hadoop/hadoop");
		conf.set("mapreduce.reduce.env", "/opt/hadoop/hadoop");
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static WordCount prepare(String[] argv) throws IOException {
		if (argv.length < 2) {
			System.out.println("at least input file/directory and output directory");
			return null;
		}

		Path inputPath = new Path(argv[0]);
		outputPath = new Path(argv[1]);
		outputFile = new Path(argv[1] + PART_R_00000);
		conf = new Configuration();
		HdfsUtil.deleteHdfsFile(conf, outputPath);

		WordCount wc = new WordCount();
		job = Job.getInstance(conf, APP_NAME);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return wc;

	}

	public void printTotalWordsReport(BufferedReader br) throws IOException {
		System.out.printf("Total words %d\n", HdfsUtil.total(br));
	}

	public void printTotalUniqueWordsReport(BufferedReader br) throws IOException {
		System.out.printf("Total unique words %d\n", HdfsUtil.totalUnique(br));
	}

	public static void main(String[] argv) throws Exception {
		WordCount wc = prepare(argv);
		wc.run(job);
		FileSystem fs = FileSystem.get(conf);
		try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputFile)))) {
			wc.printTotalWordsReport(br);
		}
		try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputFile)))) {
			wc.printTotalUniqueWordsReport(br);
		}

	}
}
