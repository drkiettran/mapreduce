package com.drkiettran.mapreduce;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.BasicConfigurator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TokenizerMapperTest {
	private final String SHAKESPEARE_DATA = "    We cannot all be masters, nor all masters\n";

	private Context context;
	private Text text;
	private LongWritable key;

	private TokenizerMapper tm;

	@BeforeEach
	public void init() {
		BasicConfigurator.configure();
		context = Mockito.mock(Context.class);
		text = new Text("");
		key = new LongWritable(0);
		tm = new TokenizerMapper();
	}

	@Test
	public void shouldDoNothing() throws IOException, InterruptedException {
		FlightsByCarriersMapper fbcm = new FlightsByCarriersMapper();
		LongWritable key = new LongWritable(0);

		tm.map(key, text, context);
	}

	@Test
	public void shouldContextWrite() throws IOException, InterruptedException {
		text = new Text(SHAKESPEARE_DATA);
		key = new LongWritable(0);
		tm.map(key, text, context);
		Mockito.verify(context, Mockito.times(SHAKESPEARE_DATA.trim().split(" ").length)).write(Mockito.any(Text.class),
				Mockito.any(IntWritable.class));
	}

	@Test
	public void shouldWriteTextAndIntWritable() throws IOException, InterruptedException {
		key = new LongWritable(1);
		text = new Text("Hello");
		ArgumentCaptor<Text> textCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<IntWritable> intWritableCaptor = ArgumentCaptor.forClass(IntWritable.class);
		tm.map(key, text, context);

		Mockito.verify(context).write(textCaptor.capture(), intWritableCaptor.capture());

		assertThat(textCaptor.getValue(), is(new Text("hello")));
		assertThat(intWritableCaptor.getValue(), is(new IntWritable(1)));
	}
}
