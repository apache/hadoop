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
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.raid.protocol.PolicyInfo;

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
  final Random rand = new Random();

  {
    ((Log4JLogger)RaidNode.LOG).getLogger().setLevel(Level.ALL);
  }


  Configuration conf;
  String namenode = null;
  String hftp = null;
  MiniDFSCluster dfs = null;
  MiniMRCluster mr = null;
  FileSystem fileSys = null;
  String jobTrackerName = null;

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
    if (local) {
      conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    } else {
      conf.set("raid.classname", "org.apache.hadoop.raid.DistRaidNode");
    }

    conf.set("raid.server.address", "localhost:0");

    // create a dfs and map-reduce cluster
    final int taskTrackers = 4;
    final int jobTrackerPort = 60050;

    dfs = new MiniDFSCluster(conf, 3, true, null);
    dfs.waitActive();
    fileSys = dfs.getFileSystem();
    namenode = fileSys.getUri().toString();
    mr = new MiniMRCluster(taskTrackers, namenode, 3);
    jobTrackerName = "localhost:" + mr.getJobTrackerPort();
    hftp = "hftp://localhost.localdomain:" + dfs.getNameNodePort();

    FileSystem.setDefaultUri(conf, namenode);
    conf.set("mapred.job.tracker", jobTrackerName);
  }
    
  /**
   * create raid.xml file for RaidNode
   */
  private void mySetup(long targetReplication,
    long metaReplication, long stripeLength) throws Exception {
    int harDelay = 1; // 1 day.
    mySetup(targetReplication, metaReplication, stripeLength, harDelay);
  }

  private void mySetup(long targetReplication,
    long metaReplication, long stripeLength, int harDelay) throws Exception {
    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str = "<configuration> " +
                   "<srcPath prefix=\"/user/dhruba/raidtest\"> " +
                     "<policy name = \"RaidTest1\"> " +
                        "<erasureCode>xor</erasureCode> " +
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
                        "<property> " +
                          "<name>time_before_har</name> " +
                          "<value> " + harDelay + "</value> " +
                          "<description> amount of time waited before har'ing parity files" +
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
    if (mr != null) { mr.shutdown(); }
    if (dfs != null) { dfs.shutdown(); }
  }

  /**
   * Test that parity files that do not have an associated master file
   * get deleted.
   */
  public void testPurge() throws Exception {
    LOG.info("Test testPurge  started.");

    long blockSizes    []  = {1024L};
    long stripeLengths []  = {5};
    long targetReplication = 1;
    long metaReplication   = 1;
    int  numBlock          = 9;
    int  iter = 0;

    createClusters(true);
    try {
      for (long blockSize : blockSizes) {
        for (long stripeLength : stripeLengths) {
           doTestPurge(iter, targetReplication, metaReplication,
                       stripeLength, blockSize, numBlock);
           iter++;
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
  private void doTestPurge(int iter, long targetReplication,
                          long metaReplication, long stripeLength,
                          long blockSize, int numBlock) throws Exception {
    LOG.info("doTestPurge started---------------------------:" +  " iter " + iter +
             " blockSize=" + blockSize + " stripeLength=" + stripeLength);
    mySetup(targetReplication, metaReplication, stripeLength);
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
      Configuration localConf = new Configuration(conf);
      localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
      cnode = RaidNode.createRaidNode(null, localConf);
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
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("doTestPurge delete file " + file1);
      fileSys.delete(file1, true);
    }
    LOG.info("doTestPurge completed:" + " blockSize=" + blockSize +
             " stripeLength=" + stripeLength);
  }

  /**
   * Create a file, wait for parity file to get HARed. Then modify the file,
   * wait for the HAR to get purged.
   */
  public void testPurgeHar() throws Exception {
    LOG.info("testPurgeHar started");
    int harDelay = 0;
    createClusters(true);
    mySetup(1, 1, 5, harDelay);
    Path dir = new Path("/user/dhruba/raidtest/");
    Path destPath = new Path("/destraid/user/dhruba/raidtest");
    Path file1 = new Path(dir + "/file");
    RaidNode cnode = null;
    try {
      TestRaidNode.createOldFile(fileSys, file1, 1, 8, 8192L);
      LOG.info("testPurgeHar created test files");

      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
      cnode = RaidNode.createRaidNode(null, localConf);

      // Wait till har is created.
      while (true) {
        try {
          FileStatus[] listPaths = listPaths = fileSys.listStatus(destPath);
          if (listPaths != null && listPaths.length == 1) {
            FileStatus s = listPaths[0];
            LOG.info("testPurgeHar found path " + s.getPath());
            if (s.getPath().toString().endsWith(".har")) {
              break;
            }
          }
        } catch (FileNotFoundException e) {
          //ignore
        }
        Thread.sleep(1000);                  // keep waiting
      }

      // Set an old timestamp.
      fileSys.setTimes(file1, 0, 0);

      boolean found = false;
      FileStatus[] listPaths = null;
      while (!found || listPaths == null || listPaths.length > 1) {
        try {
          listPaths = fileSys.listStatus(destPath);
        } catch (FileNotFoundException e) {
          // If the parent directory is deleted because the har is deleted
          // and the parent is empty, try again.
          Thread.sleep(1000);
          continue;
        }
        if (listPaths != null) {
          for (FileStatus s: listPaths) {
            LOG.info("testPurgeHar waiting for parity file to be recreated" +
              " and har to be deleted found " + s.getPath());
            if (s.getPath().toString().endsWith("file") &&
                s.getModificationTime() == 0) {
              found = true;
            }
          }
        }
        Thread.sleep(1000);
      }
    } catch (Exception e) {
      LOG.info("testPurgeHar Exception " + e +
          StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      fileSys.delete(dir, true);
      fileSys.delete(destPath, true);
      stopClusters();
    }
  }

  /**
   * Create parity file, delete original file's directory and then validate that
   * parity directory is automatically deleted.
   */
  public void testPurgeDirectory() throws Exception {
    long stripeLength = 5;
    long blockSize = 8192;
    long targetReplication = 1;
    long metaReplication   = 1;
    int  numBlock          = 9;

    createClusters(true);
    mySetup(targetReplication, metaReplication, stripeLength);
    Path dir = new Path("/user/dhruba/raidtest/");
    Path file1 = new Path(dir + "/file1");
    RaidNode cnode = null;
    try {
      TestRaidNode.createOldFile(fileSys, file1, 1, numBlock, blockSize);

      // create an instance of the RaidNode
      Configuration localConf = new Configuration(conf);
      localConf.set(RaidNode.RAID_LOCATION_KEY, "/destraid");
      cnode = RaidNode.createRaidNode(null, localConf);

      Path destPath = new Path("/destraid/user/dhruba/raidtest");
      TestRaidDfs.waitForFileRaided(LOG, fileSys, file1, destPath);

      // delete original directory.
      assertTrue("Unable to delete original directory " + file1 ,
                 fileSys.delete(file1.getParent(), true));
      LOG.info("deleted file " + file1);

      // wait till parity file and directory are automatically deleted
      long start = System.currentTimeMillis();
      while (fileSys.exists(destPath) &&
            System.currentTimeMillis() - start < 120000) {
        LOG.info("testPurgeDirectory waiting for parity files to be removed.");
        Thread.sleep(1000);                  // keep waiting
      }
      assertFalse(fileSys.exists(destPath));

    } catch (Exception e) {
      LOG.info("testPurgeDirectory Exception " + e +
                                          StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cnode != null) { cnode.stop(); cnode.join(); }
      LOG.info("testPurgeDirectory delete file " + file1);
      fileSys.delete(file1, true);
      stopClusters();
    }
  }

  /**
   * Test that an XOR parity file is removed when a RS parity file is detected.
   */
  public void testPurgePreference() throws Exception {
    createClusters(true);
    Path dir = new Path("/user/test/raidtest/");
    Path file1 = new Path(dir + "/file1");

    PolicyInfo infoXor = new PolicyInfo("testPurgePreference", conf);
    infoXor.setSrcPath("/user/test/raidtest");
    infoXor.setErasureCode("xor");
    infoXor.setDescription("test policy");
    infoXor.setProperty("targetReplication", "2");
    infoXor.setProperty("metaReplication", "2");

    PolicyInfo infoRs = new PolicyInfo("testPurgePreference", conf);
    infoRs.setSrcPath("/user/test/raidtest");
    infoRs.setErasureCode("rs");
    infoRs.setDescription("test policy");
    infoRs.setProperty("targetReplication", "1");
    infoRs.setProperty("metaReplication", "1");
    try {
      TestRaidNode.createOldFile(fileSys, file1, 1, 9, 8192L);
      FileStatus stat = fileSys.getFileStatus(file1);

      // Create the parity files.
      RaidNode.doRaid(
        conf, infoXor, stat, new RaidNode.Statistics(), Reporter.NULL);
      RaidNode.doRaid(
        conf, infoRs, stat, new RaidNode.Statistics(), Reporter.NULL);
      Path xorParity =
        new Path(RaidNode.DEFAULT_RAID_LOCATION, "user/test/raidtest/file1");
      Path rsParity =
        new Path(RaidNode.DEFAULT_RAIDRS_LOCATION, "user/test/raidtest/file1");
      assertTrue(fileSys.exists(xorParity));
      assertTrue(fileSys.exists(rsParity));

      // Check purge of a single parity file.
      RaidNode cnode = RaidNode.createRaidNode(conf);
      FileStatus raidRsStat =
        fileSys.getFileStatus(new Path(RaidNode.DEFAULT_RAIDRS_LOCATION));
      cnode.purgeMonitor.recursePurge(infoRs.getErasureCode(), fileSys, fileSys,
         RaidNode.DEFAULT_RAIDRS_LOCATION, raidRsStat);

      // Calling purge under the RS path has no effect.
      assertTrue(fileSys.exists(xorParity));
      assertTrue(fileSys.exists(rsParity));

      FileStatus raidStat =
         fileSys.getFileStatus(new Path(RaidNode.DEFAULT_RAID_LOCATION));
      cnode.purgeMonitor.recursePurge(infoXor.getErasureCode(), fileSys, fileSys,
         RaidNode.DEFAULT_RAID_LOCATION, raidStat);
      // XOR parity must have been purged by now.
      assertFalse(fileSys.exists(xorParity));
      assertTrue(fileSys.exists(rsParity));

      // Now check the purge of a parity har.
      // Delete the RS parity for now.
      fileSys.delete(rsParity);
      // Recreate the XOR parity.
      Path xorHar =
        new Path(RaidNode.DEFAULT_RAID_LOCATION, "user/test/raidtest/raidtest" +
          RaidNode.HAR_SUFFIX);
      RaidNode.doRaid(
        conf, infoXor, stat, new RaidNode.Statistics(), Reporter.NULL);
      assertTrue(fileSys.exists(xorParity));
      assertFalse(fileSys.exists(xorHar));

      // Create the har.
      long cutoff = System.currentTimeMillis();
      cnode.recurseHar(infoXor, fileSys, raidStat,
        RaidNode.DEFAULT_RAID_LOCATION, fileSys, cutoff,
        RaidNode.tmpHarPathForCode(conf, infoXor.getErasureCode()));

      // Call purge to get rid of the parity file. The har should remain.
      cnode.purgeMonitor.recursePurge(infoXor.getErasureCode(), fileSys, fileSys,
         RaidNode.DEFAULT_RAID_LOCATION, raidStat);
      // XOR har should exist but xor parity file should have been purged.
      assertFalse(fileSys.exists(xorParity));
      assertTrue(fileSys.exists(xorHar));

      // Now create the RS parity.
      RaidNode.doRaid(
        conf, infoRs, stat, new RaidNode.Statistics(), Reporter.NULL);
      cnode.purgeMonitor.recursePurge(infoXor.getErasureCode(), fileSys, fileSys,
         RaidNode.DEFAULT_RAID_LOCATION, raidStat);
      // XOR har should get deleted.
      assertTrue(fileSys.exists(rsParity));
      assertFalse(fileSys.exists(xorParity));
      assertFalse(fileSys.exists(xorHar));

    } finally {
      stopClusters();
    }
  }
}
