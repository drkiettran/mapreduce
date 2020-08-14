package com.drkiettran.mapreduce;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class WordCountTest {
	private Job mockJob;
	private WordCount wc;

	@BeforeEach
	public void setUp() throws IOException, ClassNotFoundException, InterruptedException {
		// Arrange/Prepare
		Path mockOutPath = Mockito.mock(Path.class);
		FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
		mockJob = Mockito.mock(Job.class);
		Mockito.when(mockOutPath.getFileSystem(Mockito.any())).thenReturn(mockFileSystem);
		wc = new WordCount();
	}

	@Test
	public void shouldRunJob()
			throws IOException, IllegalArgumentException, ClassNotFoundException, InterruptedException {
		Mockito.when(mockJob.waitForCompletion(true)).thenReturn(false);
		MatcherAssert.assertThat(wc.run(mockJob), equalTo(1));
	}

	@Test
	public void shouldNotRunJob()
			throws IOException, IllegalArgumentException, ClassNotFoundException, InterruptedException {
		Mockito.when(mockJob.waitForCompletion(true)).thenReturn(true);
		MatcherAssert.assertThat(wc.run(mockJob), equalTo(0));
	}

	@Test
	public void shouldPrepare() throws IOException {
		String[] argv = { "inPath", "outPath" };
		MatcherAssert.assertThat(WordCount.prepare(argv), IsNull.notNullValue());
	}

	@Test
	public void shouldNotPrepare() throws IOException {
		String[] argv = { "outPath" };
		MatcherAssert.assertThat(WordCount.prepare(argv), IsNull.nullValue());
	}

	@Test
	public void shouldPrintReport() throws IOException {
		try (BufferedReader br = new BufferedReader(new StringReader("abc\t1\ndef\t2"))) {
			wc.printTotalWordsReport(br);
		}

		try (BufferedReader br = new BufferedReader(new StringReader("abc\t1\ndef\t2"))) {
			wc.printTotalUniqueWordsReport(br);
		}

	}
}
