package com.drkiettran.mapreduce;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class HdfsUtilTest {
	@Test
	public void shouldDeleteHdfsFolder() throws IOException {
		// Arrange/Prepare
		Configuration mockConf = Mockito.mock(Configuration.class);
		Path mockOutPath = Mockito.mock(Path.class);
		FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
		Mockito.when(mockOutPath.getFileSystem(Mockito.any())).thenReturn(mockFileSystem);
		Mockito.when(mockFileSystem.delete(mockOutPath, true)).thenReturn(true);

		// Act & Assert
		MatcherAssert.assertThat(HdfsUtil.deleteHdfsFile(mockConf, mockOutPath), equalTo(true));
	}

	@Test
	public void shouldCallConstructor() {
		MatcherAssert.assertThat(new HdfsUtil(), IsNull.notNullValue());
	}

	@Test
	public void shouldTotal() throws IOException, ClassNotFoundException, InterruptedException {
		BufferedReader br = new BufferedReader(new StringReader("abc\t1\ndef\t2"));
		try {
			MatcherAssert.assertThat(HdfsUtil.total(br), equalTo(3L));
		} finally {
			br.close();
		}
	}

	@Test
	public void shouldTotalUnique() throws IOException, ClassNotFoundException, InterruptedException {
		BufferedReader br = new BufferedReader(new StringReader("abc\t1\ndef\t2"));
		try {
			MatcherAssert.assertThat(HdfsUtil.totalUnique(br), equalTo(2L));
		} finally {
			br.close();
		}
	}

}
