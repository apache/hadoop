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
package org.apache.hadoop.hdfs;

import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.zip.CRC32;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedRaidFileSystem;
import org.apache.hadoop.raid.RaidNode;

public class TestRaidDfs extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.raid.TestRaidNode");
  final Random rand = new Random();
  final static int NUM_DATANODES = 3;

  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;
  RaidNode cnode = null;
  String jobTrackerName = null;

  private void mySetup() throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // do not use map-reduce cluster for Raiding
    conf.setBoolean("fs.raidnode.local", true);
    conf.set("raid.server.address", "localhost:0");
    conf.setInt("hdfs.raid.stripeLength", 3);
    conf.set("hdfs.raid.locations", "/destraid");

    dfs = new MiniDFSCluster(conf, NUM_DATANODES, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    
    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                   "<srcPath prefix=\"/user/dhruba/raidtest\"> " +
                     "<policy name = \"RaidTest1\"> " +
                        "<destPath> /destraid</destPath> " +
                        "<property> " +
                          "<name>targetReplication</name> " +
                          "<value>1</value> " + 
                          "<description>after RAIDing, decrease the replication factor of a file to this value." +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>metaReplication</name> " +
                          "<value>1</value> " + 
                          "<description> replication factor of parity file" +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>modTimePeriod</name> " +
                          "<value>2000</value> " + 
                          "<description> time (milliseconds) after a file is modified to make it " +
                                         "a candidate for RAIDing " +
                          "</description> " + 
                        "</property> " +
                     "</policy>" +
                   "</srcPath>" +
                 "</configuration>";
    fileWriter.write(str);
    fileWriter.close();
  }

  private void myTearDown() throws Exception {
    if (cnode != null) { cnode.stop(); cnode.join(); }
    if (dfs != null) { dfs.shutdown(); }
  }

  /**
   * Test DFS Raid
   */
  public void testRaidDfs() throws Exception {
    LOG.info("Test testRaidDfs started.");
    long blockSize = 8192L;
    int stripeLength = 3;
    mySetup();
    Path file1 = new Path("/user/dhruba/raidtest/file1");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    long crc1 = createOldFile(fileSys, file1, 1, 7, blockSize);
    LOG.info("Test testPathFilter created test files");

    // create an instance of the RaidNode
    cnode = RaidNode.createRaidNode(null, conf);
    
    try {
      FileStatus[] listPaths = null;

      // wait till file is raided
      while (listPaths == null || listPaths.length != 1) {
        LOG.info("Test testPathFilter waiting for files to be raided.");
        try {
          listPaths = fileSys.listStatus(destPath);
        } catch (FileNotFoundException e) {
          //ignore
        }
        Thread.sleep(1000);                  // keep waiting
      }
      assertEquals(listPaths.length, 1); // all files raided
      LOG.info("Files raided so far : " + listPaths[0].getPath());

      // extract block locations from File system. Wait till file is closed.
      LocatedBlocks locations = null;
      DistributedFileSystem dfs = (DistributedFileSystem) fileSys;
      while (true) {
        locations = dfs.getClient().getNamenode().getBlockLocations(file1.toString(),
                                                               0, listPaths[0].getLen());
        if (!locations.isUnderConstruction()) {
          break;
        }
        Thread.sleep(1000);
      }

      // filter all filesystem calls from client
      Configuration clientConf = new Configuration(conf);
      clientConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedRaidFileSystem");
      clientConf.set("fs.raid.underlyingfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
      URI dfsUri = dfs.getUri();
      FileSystem.closeAll();
      FileSystem raidfs = FileSystem.get(dfsUri, clientConf);
      
      assertTrue("raidfs not an instance of DistributedRaidFileSystem",raidfs instanceof DistributedRaidFileSystem);
      
      // corrupt first block of file
      LOG.info("Corrupt first block of file");
      corruptBlock(file1, locations.get(0).getBlock());
      validateFile(raidfs, file1, file1, crc1);

    } catch (Exception e) {
      LOG.info("testPathFilter Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      myTearDown();
    }
    LOG.info("Test testPathFilter completed.");
  }
  
  //
  // creates a file and populate it with random data. Returns its crc.
  //
  private long createOldFile(FileSystem fileSys, Path name, int repl, int numBlocks, long blocksize)
    throws IOException {
    CRC32 crc = new CRC32();
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, blocksize);
    // fill random data into file
    final byte[] b = new byte[(int)blocksize];
    for (int i = 0; i < numBlocks; i++) {
      rand.nextBytes(b);
      stm.write(b);
      crc.update(b);
    }
    stm.close();
    return crc.getValue();
  }

  //
  // validates that file matches the crc.
  //
  private void validateFile(FileSystem fileSys, Path name1, Path name2, long crc) 
    throws IOException {

    FileStatus stat1 = fileSys.getFileStatus(name1);
    FileStatus stat2 = fileSys.getFileStatus(name2);
    assertTrue(" Length of file " + name1 + " is " + stat1.getLen() + 
               " is different from length of file " + name1 + " " + stat2.getLen(),
               stat1.getLen() == stat2.getLen());

    CRC32 newcrc = new CRC32();
    FSDataInputStream stm = fileSys.open(name2);
    final byte[] b = new byte[4192];
    int num = 0;
    while (num >= 0) {
      num = stm.read(b);
      if (num < 0) {
        break;
      }
      newcrc.update(b, 0, num);
    }
    stm.close();
    LOG.info(" Newcrc " + newcrc.getValue() + " old crc " + crc);
    if (newcrc.getValue() != crc) {
      fail("CRC mismatch of files " + name1 + " with file " + name2);
    }
  }

  /*
   * The Data directories for a datanode
   */
  private File[] getDataNodeDirs(int i) throws IOException {
    File base_dir = new File(System.getProperty("test.build.data"), "dfs/");
    File data_dir = new File(base_dir, "data");
    File dir1 = new File(data_dir, "data"+(2*i+1));
    File dir2 = new File(data_dir, "data"+(2*i+2));
    if (dir1.isDirectory() && dir2.isDirectory()) {
      File[] dir = new File[2];
      dir[0] = new File(dir1, "current");
      dir[1] = new File(dir2, "current"); 
      return dir;
    }
    return new File[0];
  }

  //
  // Corrupt specified block of file
  //
  void corruptBlock(Path file, Block blockNum) throws IOException {
    long id = blockNum.getBlockId();

    // Now deliberately remove/truncate data blocks from the block.
    //
    for (int i = 0; i < NUM_DATANODES; i++) {
      File[] dirs = getDataNodeDirs(i);
      
      for (int j = 0; j < dirs.length; j++) {
        File[] blocks = dirs[j].listFiles();
        assertTrue("Blocks do not exist in data-dir", (blocks != null) && (blocks.length >= 0));
        for (int idx = 0; idx < blocks.length; idx++) {
          if (blocks[idx].getName().startsWith("blk_" + id) &&
              !blocks[idx].getName().endsWith(".meta")) {
            blocks[idx].delete();
            LOG.info("Deleted block " + blocks[idx]);
          }
        }
      }
    }
  }

}
