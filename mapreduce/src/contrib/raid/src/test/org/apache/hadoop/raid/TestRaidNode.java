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
package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.raid.protocol.PolicyList;

/**
  * Test the generation of parity blocks for files with different block
  * sizes. Also test that a data block can be regenerated from a raid stripe
  * using the parity block
  */
public class TestRaidNode extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.raid.TestRaidNode");
  final static Random rand = new Random();

  Configuration conf;
  String namenode = null;
  MiniDFSCluster dfs = null;
  FileSystem fileSys = null;

  /**
   * create mapreduce and dfs clusters
   */
  private void createClusters(boolean local) throws Exception {

    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();
    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);
    conf.setBoolean("dfs.permissions.enabled", true);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // the RaidNode does the raiding inline (instead of submitting to map/reduce)
    conf.setBoolean("fs.raidnode.local", local);

    conf.set("raid.server.address", "localhost:0");
    
    // create a dfs and map-reduce cluster
    final int taskTrackers = 4;
    final int jobTrackerPort = 60050;

    dfs = new MiniDFSCluster(conf, 3, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
  }
    
  /**
   * create raid.xml file for RaidNode
   */
  private void mySetup(String path, short srcReplication, long targetReplication,
                long metaReplication, long stripeLength) throws Exception {
    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                   "<srcPath prefix=\"" + path + "\"> " +
                     "<policy name = \"RaidTest1\"> " +
                        "<destPath> /destraid</destPath> " +
                        "<property> " +
                          "<name>srcReplication</name> " +
                          "<value>" + srcReplication + "</value> " +
                          "<description> pick only files whole replFactor is greater than or equal to " +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>targetReplication</name> " +
                          "<value>" + targetReplication + "</value> " +
                          "<description>after RAIDing, decrease the replication factor of a file to this value." +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>metaReplication</name> " +
                          "<value>" + metaReplication + "</value> " +
                          "<description> replication factor of parity file" +
                          "</description> " + 
                        "</property> " +
                        "<property> " +
                          "<name>stripeLength</name> " +
                          "<value>" + stripeLength + "</value> " +
                          "<description> the max number of blocks in a file to RAID together " +
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

  /**
   * stop clusters created earlier
   */
  private void stopClusters() throws Exception {
    if (dfs != null) { dfs.shutdown(); }
  }

  /**
   * Test to run a filter
   */
  public void testPathFilter() throws Exception {
    LOG.info("Test testPathFilter started.");

    long blockSizes    []  = {1024L};
    long stripeLengths []  = {1, 2, 5, 6, 10, 11, 12};
    long targetReplication = 1;
    long metaReplication   = 1;
    int  numBlock          = 11;
    int  iter = 0;

    try {
      createClusters(true);
      for (long blockSize : blockSizes) {
        for (long stripeLength : stripeLengths) {
           doTestPathFilter(iter, targetReplication, metaReplication,
                                              stripeLength, blockSize, numBlock);
           iter++;
        }
      }
      doCheckPolicy();
    } finally {
      stopClusters();
    }
    LOG.info("Test testPathFilter completed.");
  }

  /**
   * Test to run a filter
   */
  private void doTestPathFilter(int iter, long targetReplication,
                          long metaReplication, long stripeLength,
                          long blockSize, int numBlock) throws Exception {
    LOG.info("doTestPathFilter started---------------------------:" +  " iter " + iter +
             " blockSize=" + blockSize + " stripeLength=" + stripeLength);
    mySetup("/user/dhruba/raidtest", (short)1, targetReplication, metaReplication, stripeLength);
    RaidShell shell = null;
    Path dir = new Path("/user/dhruba/raidtest/");
    Path file1 = new Path(dir + "/file" + iter);
    RaidNode cnode = null;
    try {
      Path destPath = new Path("/destraid/user/dhruba/raidtest");
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);
      long crc1 = createOldFile(fileSys, file1, 1, numBlock, blockSize);
      LOG.info("doTestPathFilter created test files for iteration " + iter);

      // create an instance of the RaidNode
      cnode = RaidNode.createRaidNode(null, conf);
      int times = 10;

      while (times-- > 0) {
        try {
          shell = new RaidShell(conf, cnode.getListenerAddress());
        } catch (Exception e) {
          LOG.info("doTestPathFilter unable to connect to " + 
              cnode.getListenerAddress() + " retrying....");
          Thread.sleep(1000);
          continue;
        }
        break;
      }
      LOG.info("doTestPathFilter created RaidShell.");
      FileStatus[] listPaths = null;

      // wait till file is raided
      while (true) {
        try {
          listPaths = fileSys.listStatus(destPath);
          int count = 0;
          if (listPaths != null && listPaths.length == 1) {
            for (FileStatus s : listPaths) {
              LOG.info("doTestPathFilter found path " + s.getPath());
              if (!s.getPath().toString().endsWith(".tmp")) {
                count++;
              }
            }
          }
          if (count > 0) {
            break;
          }
        } catch (FileNotFoundException e) {
          //ignore
        }
        LOG.info("doTestPathFilter waiting for files to be raided. Found " + 
                 (listPaths == null ? "none" : listPaths.length));
        Thread.sleep(1000);                  // keep waiting
      }
      // assertEquals(listPaths.length, 1); // all files raided
      LOG.info("doTestPathFilter all files found in Raid.");

      // check for error at beginning of file
      if (numBlock >= 1) {
        LOG.info("Check error at beginning of file.");
        simulateError(shell, fileSys, file1, crc1, 0);
      }

      // check for error at the beginning of second block
      if (numBlock >= 2) {
        LOG.info("Check error at beginning of second block.");
        simulateError(shell, fileSys, file1, crc1, blockSize + 1);
      }

      // check for error at the middle of third block
      if (numBlock >= 3) {
        LOG.info("Check error at middle of third block.");
        simulateError(shell, fileSys, file1, crc1, 2 * blockSize + 10);
      }

      // check for error at the middle of second stripe
      if (numBlock >= stripeLength + 1) {
        LOG.info("Check error at middle of second stripe.");
        simulateError(shell, fileSys, file1, crc1,
                                            stripeLength * blockSize + 100);
      }

    } catch (Exception e) {
      LOG.info("doTestPathFilter Exception " + e +
                                          StringUtils.stringifyException(e));
      throw e;
    } finally {
      shell.close();
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPathFilter delete file " + file1);
      fileSys.delete(file1, true);
    }
    LOG.info("doTestPathFilter completed:" + " blockSize=" + blockSize +
                                             " stripeLength=" + stripeLength);
  }

  // Check that raid occurs only on files that have a replication factor
  // greater than or equal to the specified value
  private void doCheckPolicy() throws Exception {
    LOG.info("doCheckPolicy started---------------------------:"); 
    short srcReplication = 3;
    long targetReplication = 2;
    long metaReplication = 1;
    long stripeLength = 2;
    long blockSize = 1024;
    int numBlock = 3;
    mySetup("/user/dhruba/policytest", srcReplication, targetReplication, metaReplication, stripeLength);
    RaidShell shell = null;
    Path dir = new Path("/user/dhruba/policytest/");
    Path file1 = new Path(dir + "/file1");
    Path file2 = new Path(dir + "/file2");
    RaidNode cnode = null;
    try {
      Path destPath = new Path("/destraid/user/dhruba/policytest");
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);

      // create an instance of the RaidNode
      cnode = RaidNode.createRaidNode(null, conf);
      int times = 10;

      while (times-- > 0) {
        try {
          shell = new RaidShell(conf, cnode.getListenerAddress());
        } catch (Exception e) {
          LOG.info("doCheckPolicy unable to connect to " + 
              cnode.getListenerAddress() + " retrying....");
          Thread.sleep(1000);
          continue;
        }
        break;
      }
      LOG.info("doCheckPolicy created RaidShell.");

      // this file should be picked up RaidNode
      long crc2 = createOldFile(fileSys, file2, 2, numBlock, blockSize);
      FileStatus[] listPaths = null;

      long firstmodtime = 0;
      // wait till file is raided
      while (true) {
        Thread.sleep(20000L);                  // waiting
        listPaths = fileSys.listStatus(destPath);
        int count = 0;
        if (listPaths != null && listPaths.length == 1) {
          for (FileStatus s : listPaths) {
            LOG.info("doCheckPolicy found path " + s.getPath());
            if (!s.getPath().toString().endsWith(".tmp")) {
              count++;
              firstmodtime = s.getModificationTime();
            }
          }
        }
        if (count > 0) {
          break;
        }
        LOG.info("doCheckPolicy waiting for files to be raided. Found " + 
                 (listPaths == null ? "none" : listPaths.length));
      }
      assertEquals(listPaths.length, 1);

      LOG.info("doCheckPolicy all files found in Raid the first time.");

      LOG.info("doCheckPolicy: recreating source file");
      crc2 = createOldFile(fileSys, file2, 2, numBlock, blockSize);

      FileStatus st = fileSys.getFileStatus(file2);
      assertTrue(st.getModificationTime() > firstmodtime);
      
      // wait till file is raided
      while (true) {
        Thread.sleep(20000L);                  // waiting
        listPaths = fileSys.listStatus(destPath);
        int count = 0;
        if (listPaths != null && listPaths.length == 1) {
          for (FileStatus s : listPaths) {
            LOG.info("doCheckPolicy found path " + s.getPath() + " " + s.getModificationTime());
            if (!s.getPath().toString().endsWith(".tmp") &&
                s.getModificationTime() > firstmodtime) {
              count++;
            }
          }
        }
        if (count > 0) {
          break;
        }
        LOG.info("doCheckPolicy waiting for files to be raided. Found " + 
                 (listPaths == null ? "none" : listPaths.length));
      }
      assertEquals(listPaths.length, 1);

      LOG.info("doCheckPolicy: file got re-raided as expected.");
      
    } catch (Exception e) {
      LOG.info("doCheckPolicy Exception " + e +
                                          StringUtils.stringifyException(e));
      throw e;
    } finally {
      shell.close();
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPathFilter delete file " + file1);
      fileSys.delete(file1, false);
    }
    LOG.info("doCheckPolicy completed:");
  }

  /**
   * Test dist Raid
   */
  public void testDistRaid() throws Exception {
    LOG.info("Test testDistRaid started.");
    long blockSize         = 1024L;
    long targetReplication = 2;
    long metaReplication   = 2;
    long stripeLength      = 3;
    short srcReplication = 1;

    try {
      createClusters(false);
      mySetup("/user/dhruba/raidtest", srcReplication, targetReplication, metaReplication, stripeLength);
      LOG.info("Test testDistRaid created test files");

      Path dir = new Path("/user/dhruba/raidtest/");
      Path destPath = new Path("/destraid/user/dhruba/raidtest");
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);
     
      ConfigManager configMgr  = new ConfigManager(conf);
      configMgr.reloadConfigsIfNecessary();
      LOG.info(" testDistRaid ConfigFile Loaded");

      // activate all categories
      Collection<PolicyList> all = configMgr.getAllPolicies();   
      PolicyList[] sorted = all.toArray(new PolicyList[all.size()]);
      Iterator<PolicyInfo> pi = sorted[0].getAll().iterator();
      PolicyInfo p = pi.next();
      List<FileStatus> ll = new ArrayList<FileStatus>();

      for(int i = 0 ; i < 10; i++){
        Path file = new Path("/user/dhruba/raidtest/file"+i);
        createOldFile(fileSys, file, 1, 7, blockSize);
        FileStatus st = fileSys.getFileStatus(file);
        ll.add(st);
      }
      
      DistRaid dr = new DistRaid(conf);      
      dr.addRaidPaths(p, ll);
      dr.doDistRaid();
      LOG.info("Test testDistRaid successful.");
      
    } catch (Exception e) {
      LOG.info("testDistRaid Exception " + e + StringUtils.stringifyException(e));
      throw e;
    } finally {
      stopClusters();
    }
    LOG.info("Test testDistRaid completed.");
  }
  
  //
  // simulate a corruption at specified offset and verify that eveyrthing is good
  //
  void simulateError(RaidShell shell, FileSystem fileSys, Path file1, 
                     long crc, long corruptOffset) throws IOException {
    // recover the file assuming that we encountered a corruption at offset 0
    String[] args = new String[3];
    args[0] = "recover";
    args[1] = file1.toString();
    args[2] = Long.toString(corruptOffset);
    Path recover1 = shell.recover(args[0], args, 1)[0];

    // compare that the recovered file is identical to the original one
    LOG.info("Comparing file " + file1 + " with recovered file " + recover1);
    validateFile(fileSys, file1, recover1, crc);
    fileSys.delete(recover1, false);
  }

  //
  // creates a file and populate it with random data. Returns its crc.
  //
  static long createOldFile(FileSystem fileSys, Path name, int repl, int numBlocks, long blocksize)
    throws IOException {
    CRC32 crc = new CRC32();
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, blocksize);
    // fill random data into file
    byte[] b = new byte[(int)blocksize];
    for (int i = 0; i < numBlocks; i++) {
      if (i == (numBlocks-1)) {
        b = new byte[(int)blocksize/2]; 
      }
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
    if (newcrc.getValue() != crc) {
      fail("CRC mismatch of files " + name1 + " with file " + name2);
    }
  }
}
