package com.drkiettran.mapreduce;

import java.io.IOException;

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
	private String inPath;
	private String outPath;

	public WordCount(Configuration conf, String inPath, String outPath) throws IOException {
		this.inPath = inPath;
		this.outPath = outPath;
		Path path = new Path(outPath);
		FileSystem hdfs = path.getFileSystem(conf);
		hdfs.delete(path, true);

	}

	public int run(Job job) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] argv) throws Exception {
		if (argv.length < 2) {
			System.out.println("at least input file/directory and output directory");
			System.exit(-1);
		}
		String inputPath = argv[0];
		String outputPath = argv[1];

		Configuration conf = new Configuration();
		WordCount wc = new WordCount(conf, inputPath, outputPath);
		Job job = Job.getInstance(conf, "Word Count");
		System.exit(wc.run(job));
	}
}
