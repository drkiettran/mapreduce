package com.drkiettran.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtil {
	public static boolean deleteHdfsFile(Configuration conf, Path path) throws IOException {
		FileSystem hdfs = path.getFileSystem(conf);
		return hdfs.delete(path, true);
	}

	public static Long total(BufferedReader br) throws IOException {
		Long totalFlights = 0L;
		String line = br.readLine();
		while (line != null) {
			StringTokenizer st = new StringTokenizer(line, " \t");
			String lastToken = null;
			while (st.hasMoreElements()) {
				lastToken = st.nextToken();
			}
			totalFlights += Long.valueOf(lastToken);
			line = br.readLine();
		}
		return totalFlights;
	}

	public static Long totalUnique(BufferedReader br) throws IOException {
		Long totalFlights = 0L;
		String line = br.readLine();
		while (line != null) {
			totalFlights++;
			line = br.readLine();
		}
		return totalFlights;
	}
}
