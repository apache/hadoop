/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.dfs;

import junit.framework.TestCase;
import junit.framework.AssertionFailedError;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.NameNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

/**
 * Test DFS logging
 * make sure that any namespace mutations are logged.
 * @author Hairong Kuang
 */
public class ClusterTestDFSNamespaceLogging extends TestCase implements FSConstants {
  private static final Log LOG =
      LogFactory.getLog("org.apache.hadoop.dfs.ClusterTestDFS");

  private static Configuration conf = new Configuration();

  /**
   * all DFS test files go under this base directory
   */
  private static String baseDirSpecified=conf.get("test.dfs.data", "/tmp/test-dfs");;

  /**
   * base dir as File
   */
  private static File baseDir=new File(baseDirSpecified);
  
  /**
   * name node port
   */
  int nameNodePort = conf.getInt("dfs.namenode.port", 9020);
  
  /** DFS client, datanodes, and namenode
   */
  DFSClient dfsClient;
  ArrayList dataNodeDaemons = new ArrayList();
  NameNode nameNodeDaemon;
  
  /** Log header length
   */
  private static final int DIR_LOG_HEADER_LEN = 30;
  private static final int BLOCK_LOG_HEADER_LEN = 32;
  /** DFS block size
   */
  private static final int BLOCK_SIZE = 32*1024*1024;
  
  /** Buffer size
   */
  private static final int BUFFER_SIZE = 4096;

  private BufferedReader logfh;
  private String logFile;
  
  protected void setUp() throws Exception {
    super.setUp();
    conf.setBoolean("test.dfs.same.host.targets.allowed", true);
  }

 /**
  * Remove old files from temp area used by this test case and be sure
  * base temp directory can be created.
  */
  protected void prepareTempFileSpace() {
    if (baseDir.exists()) {
      try { // start from a blank state
        FileUtil.fullyDelete(baseDir);
      } catch (Exception ignored) {
      }
    }
    baseDir.mkdirs();
    if (!baseDir.isDirectory()) {
      throw new RuntimeException("Value of root directory property" 
          + "test.dfs.data for dfs test is not a directory: "
          + baseDirSpecified);
    }
  }

  /**
   * Pseudo Distributed FS Test.
   * Test DFS by running all the necessary daemons in one process.
   *
   * @throws Exception
   */
  public void testFsPseudoDistributed() throws Exception {
	  // test on a small cluster with 3 data nodes
	  testFsPseudoDistributed(3);
  }
  
  private void testFsPseudoDistributed( int datanodeNum ) throws Exception {
    try {
      prepareTempFileSpace();

      configureDFS();
      startDFS(datanodeNum);

      if( logfh == null )
        try {
          logfh = new BufferedReader( new FileReader( logFile ) );
        } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          throw new AssertionFailedError("Log file does not exist: "+logFile);
        }
    
      // create a directory
      try {
        assertTrue(dfsClient.mkdirs( new UTF8( "/data") ));
        assertMkdirs( "/data", false );
      } catch ( IOException ioe ) {
      	ioe.printStackTrace();
      }
       
      try {
        assertTrue(dfsClient.mkdirs( new UTF8( "data") ));
        assertMkdirs( "data", true );
      } catch ( IOException ioe ) {
       	ioe.printStackTrace();
      }
      
      //
      // create a file with 1 data block
      try {
        createFile("/data/xx", 1);
        assertCreate( "/data/xx", 1, false );
      } catch( IOException ioe ) {
    	assertCreate( "/data/xx", 1, true );
      }
    
      // create a file with 2 data blocks
      try {
        createFile("/data/yy", BLOCK_SIZE+1);
        assertCreate( "/data/yy", BLOCK_SIZE+1, false );
      } catch( IOException ioe ) {
    	assertCreate( "/data/yy", BLOCK_SIZE+1, true );
      }

      // create an existing file
      try {
        createFile("/data/xx", 2);
        assertCreate( "/data/xx", 2, false );
      } catch( IOException ioe ) {
      	assertCreate( "/data/xx", 2, true );
      }
    
      // delete the file
      try {
        dfsClient.delete( new UTF8("/data/yy") );
        assertDelete("/data/yy", false);
      } catch( IOException ioe ) {
        ioe.printStackTrace();
      }

    
      // rename the file
      try {
        dfsClient.rename( new UTF8("/data/xx"), new UTF8("/data/yy") );
        assertRename( "/data/xx", "/data/yy", false );
      } catch( IOException ioe ) {
      	ioe.printStackTrace();
      }

      try {
        dfsClient.delete(new UTF8("/data/xx"));
        assertDelete("/data/xx", true);
      } catch(IOException ioe) {
    	ioe.printStackTrace();
      }
      
      try {
        dfsClient.rename( new UTF8("/data/xx"), new UTF8("/data/yy") );    
        assertRename( "/data/xx", "/data/yy", true );
      } catch( IOException ioe) {
    	ioe.printStackTrace();
      }
        
    } catch (AssertionFailedError afe) {
      afe.printStackTrace();
      throw afe;
    } catch (Throwable t) {
      msg("Unexpected exception_a: " + t);
      t.printStackTrace();
    } finally {
      shutdownDFS();

    }
  }

  private void createFile( String filename, long fileSize ) throws IOException { 
    //
    //           write filesize of data to file
    //
    byte[] buffer = new byte[BUFFER_SIZE];
    UTF8 testFileName = new UTF8(filename); // hardcode filename
    FSOutputStream nos;
	nos = dfsClient.create(testFileName, false);
    try {
      for (long nBytesWritten = 0L;
                nBytesWritten < fileSize;
                nBytesWritten += buffer.length) {
        if ((nBytesWritten + buffer.length) > fileSize) {
          int pb = (int) (fileSize - nBytesWritten);
          byte[] bufferPartial = new byte[pb];
          for( int i=0; i<pb; i++) {
            bufferPartial[i]='a';
          }
          nos.write(buffer);
        } else {
          for( int i=0; i<buffer.length;i++) {
            buffer[i]='a';
          }
          nos.write(buffer);
        }
      }
    } finally {
      nos.flush();
      nos.close();
    }
  }

  private void assertMkdirs( String fileName, boolean failed ) {
	  assertHasLogged("NameNode.mkdirs: " +fileName, DIR_LOG_HEADER_LEN+1);
	  assertHasLogged("NameSystem.mkdirs: "+fileName, DIR_LOG_HEADER_LEN);
	  if( failed )
		assertHasLogged("FSDirectory.mkdirs: "
        			+"failed to create directory "+fileName, DIR_LOG_HEADER_LEN);
	  else
	    assertHasLogged( "FSDirectory.mkdirs: created directory "+fileName, DIR_LOG_HEADER_LEN);
  }
  
  private void assertCreate( String fileName, int filesize, boolean failed ) {
	  assertHasLogged("NameNode.create: file "+fileName, DIR_LOG_HEADER_LEN+1);
	  assertHasLogged("NameSystem.startFile: file "+fileName, DIR_LOG_HEADER_LEN);
	  if( failed ) {
		assertHasLogged("NameSystem.startFile: "
            		  +"failed to create file " + fileName, DIR_LOG_HEADER_LEN);
	  } else {
	    assertHasLogged("NameSystem.allocateBlock: "+fileName, BLOCK_LOG_HEADER_LEN);
	    int blockNum = (filesize/BLOCK_SIZE*BLOCK_SIZE==filesize)?
		  filesize/BLOCK_SIZE : 1+filesize/BLOCK_SIZE;
	    for( int i=1; i<blockNum; i++) {
		  assertHasLogged("NameNode.addBlock: file "+fileName, BLOCK_LOG_HEADER_LEN+1);
		  assertHasLogged("NameSystem.getAdditionalBlock: file "+fileName, BLOCK_LOG_HEADER_LEN);
		  assertHasLogged("NameSystem.allocateBlock: "+fileName, BLOCK_LOG_HEADER_LEN);
	    }
	    assertHasLogged("NameNode.complete: "+fileName, DIR_LOG_HEADER_LEN+1);
	    assertHasLogged("NameSystem.completeFile: "+fileName, DIR_LOG_HEADER_LEN);
	    assertHasLogged("FSDirectory.addFile: "+fileName+" with "
			  +blockNum+" blocks is added to the file system", DIR_LOG_HEADER_LEN);
	    assertHasLogged("NameSystem.completeFile: "+fileName
			  +" is removed from pendingCreates", DIR_LOG_HEADER_LEN);
	  }
  }
  
  private void assertDelete( String fileName, boolean failed ) {
	  assertHasLogged("NameNode.delete: "+fileName, DIR_LOG_HEADER_LEN+1);
      assertHasLogged("NameSystem.delete: "+fileName, DIR_LOG_HEADER_LEN);
      assertHasLogged("FSDirectory.delete: "+fileName, DIR_LOG_HEADER_LEN);
      if( failed )
        assertHasLogged("FSDirectory.unprotectedDelete: "
            +"failed to remove "+fileName, DIR_LOG_HEADER_LEN );
      else
        assertHasLogged("FSDirectory.unprotectedDelete: "
            +fileName+" is removed", DIR_LOG_HEADER_LEN);
  }
  
  private void assertRename( String src, String dst, boolean failed ) {
	  assertHasLogged("NameNode.rename: "+src+" to "+dst, DIR_LOG_HEADER_LEN+1);
	  assertHasLogged("NameSystem.renameTo: "+src+" to "+dst, DIR_LOG_HEADER_LEN );
	  assertHasLogged("FSDirectory.renameTo: "+src+" to "+dst, DIR_LOG_HEADER_LEN );
	  if( failed )
		assertHasLogged("FSDirectory.unprotectedRenameTo: "
                         +"failed to rename "+src+" to "+dst, DIR_LOG_HEADER_LEN);
	  else
	    assertHasLogged("FSDirectory.unprotectedRenameTo: "
                       +src+" is renamed to "+dst, DIR_LOG_HEADER_LEN );
  }
  
  private void assertHasLogged( String target, int headerLen ) {
	  String line;
	  boolean notFound = true;
	  try {
	      while( notFound && (line=logfh.readLine()) != null ) {
		      if(line.length()>headerLen && line.startsWith(target, headerLen))
			      notFound = false;
	      }
	  } catch(java.io.IOException e) {
		  throw new AssertionFailedError("error reading the log file");
	  }
	  if(notFound) {
		  throw new AssertionFailedError(target+" not logged");
	  }
  }

  //
  //     modify config for test
  //
  private void configureDFS() throws IOException {
	// set given config param to override other config settings
	conf.setInt("dfs.block.size", BLOCK_SIZE);
	// verify that config changed
	assertTrue(BLOCK_SIZE == conf.getInt("dfs.block.size", 2)); // 2 is an intentional obviously-wrong block size
	// downsize for testing (just to save resources)
	conf.setInt("dfs.namenode.handler.count", 3);
	conf.setLong("dfs.blockreport.intervalMsec", 50*1000L);
	conf.setLong("dfs.datanode.startupMsec", 15*1000L);
	conf.setInt("dfs.replication", 2);
	System.setProperty("hadoop.log.dir", baseDirSpecified+"/logs");
	conf.setInt("hadoop.logfile.count", 1);
	conf.setInt("hadoop.logfile.size", 1000000000);
  }
  
  private void startDFS( int dataNodeNum) throws IOException {
    //
    //          start a NameNode
    String nameNodeSocketAddr = "localhost:" + nameNodePort;
    conf.set("fs.default.name", nameNodeSocketAddr);
    
    String nameFSDir = baseDirSpecified + "/name";
	conf.set("dfs.name.dir", nameFSDir);
	
    NameNode.format(conf);
    
    nameNodeDaemon = new NameNode(new File[] { new File(nameFSDir) },
        "localhost", nameNodePort, conf);

     //
      //        start DataNodes
      //
      for (int i = 0; i < dataNodeNum; i++) {
        // uniquely config real fs path for data storage for this datanode
        String dataDir[] = new String[1];
        dataDir[0] = baseDirSpecified + "/datanode" + i;
        conf.set("dfs.data.dir", dataDir[0]);
        DataNode dn = DataNode.makeInstance(dataDir, conf);
        if (dn != null) {
          dataNodeDaemons.add(dn);
          (new Thread(dn, "DataNode" + i + ": " + dataDir[0])).start();
        }
      }
	         
      assertTrue("incorrect datanodes for test to continue",
            (dataNodeDaemons.size() == dataNodeNum));
      //
      //          wait for datanodes to report in
      try {
        awaitQuiescence();
      } catch( InterruptedException e) {
    	  e.printStackTrace();
      }
      
      //  act as if namenode is a remote process
      dfsClient = new DFSClient(new InetSocketAddress("localhost", nameNodePort), conf);
  }

  private void shutdownDFS() {
      // shutdown client
      if (dfsClient != null) {
        try {
          msg("close down subthreads of DFSClient");
          dfsClient.close();
        } catch (Exception ignored) { }
        msg("finished close down of DFSClient");
      }

      //
      // shut down datanode daemons (this takes advantage of being same-process)
      msg("begin shutdown of all datanode daemons" );

      for (int i = 0; i < dataNodeDaemons.size(); i++) {
        DataNode dataNode = (DataNode) dataNodeDaemons.get(i);
        try {
          dataNode.shutdown();
        } catch (Exception e) {
           msg("ignoring exception during (all) datanode shutdown, e=" + e);
        }
      }
      msg("finished shutdown of all datanode daemons");
      
      // shutdown namenode
      msg("begin shutdown of namenode daemon");
      try {
        nameNodeDaemon.stop();
      } catch (Exception e) {
        msg("ignoring namenode shutdown exception=" + e);
      }
      msg("finished shutdown of namenode daemon");
  }
  
  /** Wait for the DFS datanodes to become quiescent.
   * The initial implementation is to sleep for some fixed amount of time,
   * but a better implementation would be to really detect when distributed
   * operations are completed.
   * @throws InterruptedException
   */
  private void awaitQuiescence() throws InterruptedException {
    // ToDo: Need observer pattern, not static sleep
    // Doug suggested that the block report interval could be made shorter
    //   and then observing that would be a good way to know when an operation
    //   was complete (quiescence detect).
    sleepAtLeast(30000);
  }

  private void msg(String s) {
    //System.out.println(s);
    LOG.info(s);
  }

  public static void sleepAtLeast(int tmsec) {
    long t0 = System.currentTimeMillis();
    long t1 = t0;
    long tslept = t1 - t0;
    while (tmsec > tslept) {
      try {
        long tsleep = tmsec - tslept;
        Thread.sleep(tsleep);
        t1 = System.currentTimeMillis();
      }  catch (InterruptedException ie) {
        t1 = System.currentTimeMillis();
      }
      tslept = t1 - t0;
    }
  }

  public static void main(String[] args) throws Exception {
    String usage = "Usage: ClusterTestDFSNameSpaceChangeLogging (no args)";
    if (args.length != 0) {
      System.err.println(usage);
      System.exit(-1);
    }
    String[] testargs = {"org.apache.hadoop.dfs.ClusterTestDFSNameSpaceChangeLogging"};
    junit.textui.TestRunner.main(testargs);
  }

}
