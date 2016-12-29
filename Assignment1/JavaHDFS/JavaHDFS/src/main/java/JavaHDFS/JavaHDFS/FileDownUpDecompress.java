package JavaHDFS.JavaHDFS;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class FileDownUpDecompress {

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	public static void main(String args[])throws Exception 
	{
		InputStream in = null;
		OutputStream out = null;
		Configuration conf = new Configuration();
		FileSystem fs;
		for(int i=0;i<args.length;i++)
		{
			try {
				String uri=args[i];
				java.nio.file.Path fileName = Paths.get(uri);
				String outputFileName = fileName.getFileName().toString();
				outputFileName = outputFileName.substring(0, outputFileName.length()-4);
				System.out.println(outputFileName);

				//copying file from URL and storing as sample.bz2
				in = new URL(args[i]).openStream();
				fs= FileSystem.get(URI.create("sample.bz2"), conf);
				out = fs.create(new Path("sample.bz2"), new Progressable() {
					public void progress() {
						System.out.print(".");
					}
				});

				IOUtils.copyBytes(in, out, 4096, true);

				System.out.println("copied file");

				//reading from sample.bz2 and compress and store as last name in URL
				fs = FileSystem.get(URI.create("hdfs://localhost:9000/user/root/sample.bz2"), conf);
				Path inputPath = new Path("/user/root/sample.bz2");
				CompressionCodecFactory factory = new CompressionCodecFactory(conf);
				CompressionCodec codec = factory.getCodec(inputPath);
				in = codec.createInputStream(fs.open(inputPath));
				out = fs.create(new Path("hdfs://localhost:9000/assignment1/question1/"+ outputFileName));
				IOUtils.copyBytes(in, out, conf);
				fs.deleteOnExit(inputPath);
			} 
			finally 
			{
				IOUtils.closeStream(in);
				IOUtils.closeStream(out);
			}
		}
	}
}