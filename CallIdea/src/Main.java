import java.io.IOException;
import java.io.File;

import org.apache.commons.lang.StringUtils;
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
			pigServer.registerScript("C:/git/CallIdea/CallIdea/pigs/prepare.pig");			
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		  
}
