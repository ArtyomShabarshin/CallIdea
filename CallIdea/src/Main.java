/*
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

  import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

 public class Main {

   public static final String theFilename = "/user/hadoop/hello.txt";
   public static final String message = "Hello, world!\n";

   public static void main (String [] args) throws IOException, URISyntaxException {

     Configuration conf = new Configuration();
     FileSystem fs = FileSystem.get(new URI("hdfs://localhost/"), conf);

     Path filenamePath = new Path(theFilename);

     try {
       if (fs.exists(filenamePath)) {
         // remove the file first
         fs.delete(filenamePath);
       }

       FSDataOutputStream out = fs.create(filenamePath);
       out.writeUTF(message);
       out.close();

       FSDataInputStream in = fs.open(filenamePath);
       String messageIn = in.readUTF();
       System.out.print(messageIn);
       in.close();
     } catch (IOException ioe) {
       System.err.println("IOException during operation: " + ioe.toString());
       System.exit(1);
     }
   }
 }
*/

import java.io.IOException;
import java.io.File;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

	public static void main(String[] args) {
		
	    File file = new File("c:/sample.log");
	    String path = StringUtils.strip(file.getPath(), "file:");
		
		HdfsServiceImpl hdfsAdminService = new HdfsServiceImpl();
		FileSystem hdfs = hdfsAdminService.getFileSystem();
	    try {
			hdfs.copyFromLocalFile(false, true, new Path(path), new Path("/user/hadoop/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
