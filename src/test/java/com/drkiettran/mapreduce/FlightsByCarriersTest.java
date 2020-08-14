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

public class FlightsByCarriersTest {
	private Job mockJob;
	private FlightsByCarriers fbc;

	@BeforeEach
	public void setUp() throws IOException, ClassNotFoundException, InterruptedException {
		// Arrange/Prepare
		Path mockOutPath = Mockito.mock(Path.class);
		FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
		mockJob = Mockito.mock(Job.class);
		Mockito.when(mockOutPath.getFileSystem(Mockito.any())).thenReturn(mockFileSystem);
		Mockito.when(mockFileSystem.delete(mockOutPath, true)).thenReturn(true);
		Mockito.when(mockJob.waitForCompletion(true)).thenReturn(false);
		fbc = new FlightsByCarriers();
	}

	@Test
	public void shouldPrepare() throws IOException {
		String[] argv = { "inPath", "outPath" };
		MatcherAssert.assertThat(FlightsByCarriers.prepare(argv), IsNull.notNullValue());
	}

	@Test
	public void shouldNotPrepare() throws IOException {
		String[] argv = { "outPath" };
		MatcherAssert.assertThat(FlightsByCarriers.prepare(argv), IsNull.nullValue());
	}

	@Test
	public void shouldRunJob()
			throws IllegalArgumentException, ClassNotFoundException, IOException, InterruptedException {
		MatcherAssert.assertThat(fbc.run(mockJob), equalTo(false));
	}

	@Test
	public void shouldPrintReport() throws IOException {
		BufferedReader br = new BufferedReader(new StringReader("abc\t1\ndef\t2"));
		fbc.printReport(br);
	}
}
