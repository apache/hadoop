package org.apache.hadoop.chukwa.validationframework;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.chukwa.extraction.demux.Demux;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ToolRunner;

public class TestDemux extends TestCase
{

  int NUM_HADOOP_SLAVES = 2;
  private String DEMUX_INPUT_PATH = "demuxHdfsData/input";
  private String DEMUX_GOLD_PATH = "demuxHdfsData/gold";
  private String DEMUX_OUTPUT_PATH = "demuxHdfsData/output_";
  
  Configuration conf = null;
  FileSystem fileSys = null;
  MiniDFSCluster dfs = null;
  MiniMRCluster mr = null;
  
  protected void setUp() throws Exception
  {
   
    
    conf = new Configuration();
    System.setProperty("hadoop.log.dir", "/tmp/");
    dfs = new MiniDFSCluster(conf,NUM_HADOOP_SLAVES,true,null,null);
    dfs.waitClusterUp();
    
    fileSys = dfs.getFileSystem();
    mr = new MiniMRCluster(NUM_HADOOP_SLAVES, fileSys.getUri().toString(), 1);
   
    unpackStorage();
  }
  
    void unpackStorage() throws IOException 
    {
      String tarFile = System.getProperty("test.demux.data") +
                       "/demuxData.tgz";
      String dataDir = System.getProperty("test.build.data");
      File dfsDir = new File(dataDir, "chukwa");
      if ( dfsDir.exists() && !FileUtil.fullyDelete(dfsDir) ) {
        throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
      }
      
      FileUtil.unTar(new File(tarFile), new File(dataDir));

      // Copy to HDFS
      FileUtil.copy(new File(System.getProperty("test.build.data") +"/demuxData/"),
          fileSys, new Path("demuxHdfsData"), false, conf);
      
      // Set input/output directories
      DEMUX_OUTPUT_PATH = DEMUX_OUTPUT_PATH + System.currentTimeMillis();

      System.out.println("DEMUX_INPUT_PATH: " +DEMUX_INPUT_PATH);
      System.out.println("DEMUX_OUTPUT_PATH: " +DEMUX_OUTPUT_PATH);
//      
//      FileStatus[] testFiles = fileSys.listStatus(new Path("demuxHdfsData"));
//      for (FileStatus f: testFiles)
//      {
//        System.out.println(f.getPath().toString());
//      }
     

  }
  
  public void testDemux() {
    try
    {
      
      String[] sortArgs = {DEMUX_INPUT_PATH.toString(), DEMUX_OUTPUT_PATH.toString()};
      int res = ToolRunner.run(mr.createJobConf(), new Demux(), sortArgs);
      Assert.assertEquals(res, 0);
      
      String[] directories = new String[2];
      directories[0] = DEMUX_GOLD_PATH;
      directories[1] = DEMUX_OUTPUT_PATH;
      
      DemuxDirectoryValidator.validate(false, fileSys, conf,directories);
      
    }
    catch(Exception e)
    {
      e.printStackTrace();
      Assert.fail("Exception in TestDemux: " + e);
    }
  }
  protected void tearDown() throws Exception
  {
    if (dfs != null)
    {
      try { dfs.shutdown(); }
      catch(Exception e) 
      { /* do nothing since we're not testing the MiniDFSCluster */ }  
    }
    if (mr != null)
    {
      try { mr.shutdown(); }
      catch(Exception e) 
      { /* do nothing since we're not testing the MiniDFSCluster */ }  
    }
  }

}
