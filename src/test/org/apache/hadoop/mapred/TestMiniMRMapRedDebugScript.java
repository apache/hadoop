package org.apache.hadoop.mapred;

import java.io.*;
import java.util.*;
import java.net.URISyntaxException;
import java.net.URI;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.filecache.DistributedCache; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;

/**
 * Class to test mapred debug Script
 */
public class TestMiniMRMapRedDebugScript extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestMiniMRMapRedDebugScript.class.getName());

  private MiniMRCluster mr;
  private MiniDFSCluster dfs;
  private FileSystem fileSys;
  
  /**
   * Fail map class 
   */
  public static class MapClass extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
     public void map (LongWritable key, Text value, 
                     OutputCollector<Text, IntWritable> output, 
                     Reporter reporter) throws IOException {
       System.err.println("Bailing out");
       throw new IOException();
     }
  }

  /**
   * Reads tasklog and returns it as string after trimming it.
   * @param filter Task log filer; can be STDOUT, STDERR,
   *                SYSLOG, DEBUGOUT, DEBUGERR
   * @param taskId The task id for which the log has to collected
   * @return task log as string
   * @throws IOException
   */
  public static String readTaskLog(TaskLog.LogName  filter, String taskId)
  throws IOException {
    // string buffer to store task log
    StringBuffer result = new StringBuffer();
    int res;

    // reads the whole tasklog into inputstream
    InputStream taskLogReader = new TaskLog.Reader(taskId, filter, 0, -1);
    // construct string log from inputstream.
    byte[] b = new byte[65536];
    while (true) {
      res = taskLogReader.read(b);
      if (res > 0) {
        result.append(new String(b));
      } else {
        break;
      }
    }
    taskLogReader.close();
    
    // trim the string and return it
    String str = result.toString();
    str = str.trim();
    return str;
  }

  /**
   * Launches failed map task and debugs the failed task
   * @param conf configuration for the mapred job
   * @param inDir input path
   * @param outDir output path
   * @param debugDir debug directory where script is present
   * @param debugCommand The command to execute script
   * @param input Input text
   * @return the output of debug script 
   * @throws IOException
   */
  public String launchFailMapAndDebug(JobConf conf,
                                      Path inDir,
                                      Path outDir,
                                      Path debugDir,
                                      String debugScript,
                                      String input)
  throws IOException {

    // set up the input file system and write input text.
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      // write input into input file
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }

    // configure the mapred Job for failing map task.
    conf.setJobName("failmap");
    conf.setMapperClass(MapClass.class);        
    conf.setReducerClass(IdentityReducer.class);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);
    conf.setMapDebugScript(debugScript);
    conf.setInputPath(inDir);
    conf.setOutputPath(outDir);
    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                      "/tmp")).toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);

    // copy debug script to cache from local file system.
    FileSystem debugFs = debugDir.getFileSystem(conf);
    Path scriptPath = new Path(debugDir,"testscript.txt");
    Path cachePath = new Path("/cacheDir");
    if (!debugFs.mkdirs(cachePath)) {
      throw new IOException("Mkdirs failed to create " + cachePath.toString());
    }
    debugFs.copyFromLocalFile(scriptPath,cachePath);
    
    // add debug script as cache file 
    String fileSys = debugFs.getName();
    String scr = null;
    if (fileSys.equals("local")) {
      scr = "file://" + cachePath + "/testscript.txt#testscript";
    } else {
      scr = "hdfs://" + fileSys + cachePath + "/testscript.txt#testscript";
    }
    URI uri = null;
    try {
      uri = new URI(scr);
    } catch (URISyntaxException ur) {
      ur.printStackTrace();
    }
    DistributedCache.createSymlink(conf);
    DistributedCache.addCacheFile(uri, conf);

    RunningJob job =null;
    // run the job. It will fail with IOException.
    try {
      job = new JobClient(conf).submitJob(conf);
    } catch (IOException e) {
    	LOG.info("Running Job failed");
    	e.printStackTrace();
    }

    String jobId = job.getJobID();
    // construct the task id of first map task of failmap
    String taskId = "task_" + jobId.substring(4) + "_m_000000_0";
    // wait for the job to finish.
    while (!job.isComplete()) ;
    
    // return the output of debugout log.
    return readTaskLog(TaskLog.LogName.DEBUGOUT,taskId);
  }

  /**
   * Tests Map task's debug script
   * 
   * In this test, we launch a mapreduce program which 
   * writes 'Bailing out' to stderr and throws an exception.
   * We will run the script when tsk fails and validate 
   * the output of debug out log. 
   *
   */
  public void testMapDebugScript(){
    try {
      
      // create configuration, dfs, file system and mapred cluster 
      Configuration cnf = new Configuration();
      dfs = new MiniDFSCluster(cnf, 1, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(2, fileSys.getName(), 1);
      JobConf conf = mr.createJobConf();
      
      // intialize input, output and debug directories
      final Path debugDir = new Path("build/test/debug");
      Path inDir = new Path("testing/wc/input");
      Path outDir = new Path("testing/wc/output");
      
      // initialize debug command and input text
      String debugScript = "./testscript";
      String input = "The input";
      
      // Launch failed map task and run debug script
      String result = launchFailMapAndDebug(conf,inDir, 
                               outDir,debugDir, debugScript, input);
      
      // Assert the output of debug script.
      assertEquals("Test Script\nBailing out", result);
      
    } catch(Exception e) {
      e.printStackTrace();
      fail("Exception in testing mapred debug script");
    } finally {  
      // close file system and shut down dfs and mapred cluster
      try {
        if (fileSys != null) {
          fileSys.close();
        }
        if (dfs != null) {
          dfs.shutdown();
        }
        if (mr != null) {
          mr.shutdown();
        }
      } catch (IOException ioe) {
        LOG.info("IO exception in closing file system)" );
        ioe.printStackTrace();        			
      }
    }
  }

  public static void main(String args[]){
    TestMiniMRMapRedDebugScript tmds = new TestMiniMRMapRedDebugScript();
    tmds.testMapDebugScript();
  }
  
}

