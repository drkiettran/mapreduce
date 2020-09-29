package com.drkiettran.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// private static final Logger LOGGER =
	// LoggerFactory.getLogger(TokenizerMapper.class);
	private static final IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^A-Za-z0-9 ]", " "));
		// LOGGER.info("input: {}", context.getInputSplit());
		// System.out.println(String.format("%d words", itr.countTokens()));
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, ONE);
		}
	}
}
