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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Extracted from Hadoop for Dummies (2014)
 */
public class FlightsByCarriers {

	private static Job job;
	private static Configuration conf;
	private static Path outputPath;
	private static Path outputFile;

	public boolean run(Job job)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		job.setJarByClass(FlightsByCarriers.class);
		job.setJobName("FlightsByCarriers");

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(FlightsByCarriersMapper.class);
		job.setReducerClass(FlightsByCarriersReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true);
	}

	/**
	 * 
	 * @param argv argv[0]: input path argv[1]; output path
	 * @throws Exception
	 */
	public static void main(String[] argv) throws Exception {
		FlightsByCarriers fbc = prepare(argv);
		fbc.run(job);
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
		fbc.printReport(br);
	}

	public void printReport(BufferedReader br) throws IOException {
		System.out.println("total flights:" + HdfsUtil.total(br));
		System.out.println("total unique flights:" + HdfsUtil.totalUnique(br));
	}

	public static FlightsByCarriers prepare(String[] argv) throws IOException {
		if (argv.length < 2) {
			System.out.println("at least input file/directory and output directory");
			return null;
		}

		Path inputPath = new Path(argv[0]);
		outputPath = new Path(argv[1]);
		outputFile = new Path(argv[1] + "/part-r-00000");
		conf = new Configuration();
		HdfsUtil.deleteHdfsFile(conf, outputPath);

		FlightsByCarriers fbc = new FlightsByCarriers();
		job = Job.getInstance(conf, "Flights by Carriers");

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return fbc;
	}
}
