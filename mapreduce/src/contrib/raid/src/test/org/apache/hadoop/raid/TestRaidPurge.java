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
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

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
 * If a file gets deleted, then verify that the parity file gets deleted too.
 */
public class TestRaidPurge extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
      "build/contrib/raid/test/data")).getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, 
      "test-raid.xml").getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static Log LOG = LogFactory.getLog("org.apache.hadoop.raid.TestRaidNode");

  {
    ((Log4JLogger)RaidNode.LOG).getLogger().setLevel(Level.ALL);
  }


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

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // make all deletions not go through Trash
    conf.set("fs.shell.delete.classname", "org.apache.hadoop.dfs.DFSClient");

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
  private void mySetup(String srcPath, long targetReplication,
                long metaReplication, long stripeLength) throws Exception {
    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                   "<srcPath prefix=\"" + srcPath + "\"> " +
                     "<policy name = \"RaidTest1\"> " +
                        "<destPath> /destraid</destPath> " +
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
   * Test that parity files that do not have an associated master file
   * get deleted.
   */
  public void testPurge() throws Exception {
    LOG.info("Test testPurge  started.");

    String srcPaths    []  = { "/user/dhruba/raidtest", "/user/dhruba/raid*" };
    long blockSizes    []  = {1024L};
    long stripeLengths []  = {5};
    long targetReplication = 1;
    long metaReplication   = 1;
    int  numBlock          = 9;
    int  iter = 0;

    createClusters(true);
    try {
      for (String srcPath : srcPaths ) {
        for (long blockSize : blockSizes) {
          for (long stripeLength : stripeLengths) {
            doTestPurge(iter, srcPath, targetReplication, metaReplication,
                stripeLength, blockSize, numBlock);
            iter++;
          }
        }
      }
    } finally {
      stopClusters();
    }
    LOG.info("Test testPurge completed.");
  }

  /**
   * Create parity file, delete original file and then validate that
   * parity file is automatically deleted.
   */
  private void doTestPurge(int iter, String srcPath, long targetReplication,
                          long metaReplication, long stripeLength,
                          long blockSize, int numBlock) throws Exception {
    LOG.info("doTestPurge started---------------------------:" +  " iter " + iter +
             " blockSize=" + blockSize + " stripeLength=" + stripeLength);
    mySetup(srcPath, targetReplication, metaReplication, stripeLength);
    RaidShell shell = null;
    Path dir = new Path("/user/dhruba/raidtest/");
    Path file1 = new Path(dir + "/file" + iter);
    RaidNode cnode = null;
    try {
      Path destPath = new Path("/destraid/user/dhruba/raidtest");
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);
      TestRaidNode.createOldFile(fileSys, file1, 1, numBlock, blockSize);
      LOG.info("doTestPurge created test files for iteration " + iter);

      // create an instance of the RaidNode
      cnode = RaidNode.createRaidNode(null, conf);
      int times = 10;

      while (times-- > 0) {
        try {
          shell = new RaidShell(conf, cnode.getListenerAddress());
        } catch (Exception e) {
          LOG.info("doTestPurge unable to connect to " + 
              cnode.getListenerAddress() + " retrying....");
          Thread.sleep(1000);
          continue;
        }
        break;
      }
      LOG.info("doTestPurge created RaidShell.");
      FileStatus[] listPaths = null;

      // wait till file is raided
      while (true) {
        try {
          listPaths = fileSys.listStatus(destPath);
          int count = 0;
          if (listPaths != null && listPaths.length == 1) {
            for (FileStatus s : listPaths) {
              LOG.info("doTestPurge found path " + s.getPath());
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
        LOG.info("doTestPurge waiting for files to be raided. Found " + 
                 (listPaths == null ? "none" : listPaths.length));
        Thread.sleep(1000);                  // keep waiting
      }
      // assertEquals(listPaths.length, 1); // all files raided
      LOG.info("doTestPurge all files found in Raid.");

      // delete original file
      assertTrue("Unable to delete original file " + file1 ,
                 fileSys.delete(file1, true));
      LOG.info("deleted file " + file1);

      // wait till parity file and directory are automatically deleted
      while (fileSys.exists(destPath)) {
        LOG.info("doTestPurge waiting for parity files to be removed.");
        Thread.sleep(1000);                  // keep waiting
      }

    } catch (Exception e) {
      LOG.info("doTestPurge Exception " + e +
                                          StringUtils.stringifyException(e));
      throw e;
    } finally {
      shell.close();
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPurge delete file " + file1);
      fileSys.delete(file1, true);
    }
    LOG.info("doTestPurge completed:" + " blockSize=" + blockSize +
             " stripeLength=" + stripeLength);
  }
}
