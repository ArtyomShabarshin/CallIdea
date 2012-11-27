import java.io.BufferedReader;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class Main {

	public static void main(String[] args) {
		
	    File file = new File("C:/git/CallIdea/CallIdea/resources/TTHuaweiUMTS_Plain.csv");
	    String path = StringUtils.strip(file.getPath(), "file:");
		
		HdfsServiceImpl hdfsAdminService = new HdfsServiceImpl();
		FileSystem hdfs = hdfsAdminService.getFileSystem();				
		
	    try {
			hdfs.copyFromLocalFile(false, true, new Path(path), new Path("/user/hadoop/cdr/"));
			hdfs.delete(new Path("/user/hadoop/res"), true);

			PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
			pigServer.registerJar("C:/git/CallIdea/BAnalysis/bin/BAnalysis.jar");
			pigServer.registerJar("C:/git/CallIdea/AutoIncrement/bin/AutoIncrement.jar");
			pigServer.registerScript("C:/git/CallIdea/CallIdea/pigs/prepare.pig");						
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	   	
	    
	    Long count = numbersCount(hdfsAdminService);
	      
	    hdfsAdminService.close();
	}

	private static long numbersCount(HdfsService hdfsAdminService) {
	    FileStatus[] fsStatus = hdfsAdminService.list("/user/hadoop/res/count");
	    if(fsStatus == null || fsStatus.length == 0) return 0L;

	    for (FileStatus status : fsStatus) {
	      if ( status.isDir() || !status.getPath().toString().startsWith("part") ) {
	        continue;
	      }
	       
	      Long actual = hdfsAdminService.read(new HdfsService.ReadCallback<Long>() {

	        @Override
	        public Long read(BufferedReader reader) throws IOException {	        	
	          String line = null;
	          while ((line = reader.readLine()) != null) {
	            return Long.parseLong(line);
	          }
	          return 0L;
	        }
	        
	      }, status.getPath());
	      
	      return actual;
	    }
	    
	    return 0L;
	  }
	
	  private static void validateResult(HdfsService hdfsAdminService, String dir) {
		    FileStatus[] fsStatus = hdfsAdminService.list(dir);
		    if(fsStatus == null || fsStatus.length == 0) return;

		    for (FileStatus status : fsStatus) {
		      if ( status.isDir() || !status.getPath().toString().startsWith("part") ) {
		        continue;
		      }
		       
		      List<String> actual = hdfsAdminService.read(new HdfsService.ReadCallback<List<String>>() {

		        @Override
		        public List<String> read(BufferedReader reader) throws IOException {
		        	List<String> results = new ArrayList<String>();
		          String line = null;
		          while ((line = reader.readLine()) != null) {
		            results.add(line);
		          }
		          return results;
		        }
		        
		      }, status.getPath());
		      System.out.println("Reduced Map:" + actual);
		    }
		  }
		  
}
