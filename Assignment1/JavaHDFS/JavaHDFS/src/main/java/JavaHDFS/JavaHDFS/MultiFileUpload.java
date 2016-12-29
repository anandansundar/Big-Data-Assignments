package JavaHDFS.JavaHDFS;
//cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

//vv FileCopyWithProgress
public class MultiFileUpload {
	public static void main(String[] args) throws Exception {
		InputStream in = null;
		OutputStream out = null;
		for(int i=0;i<args.length;i++)
		{
			String localSrc = args[i];
			in = new BufferedInputStream(new FileInputStream(localSrc));
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/assignment1/Question2/"), conf);
			out = fs.create(new Path("hdfs://localhost:9000/assignment1/Question2/"+ localSrc));
			IOUtils.copyBytes(in, out, conf);
		}
		in.close();
		out.close();
	}
}
