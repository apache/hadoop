/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

import java.io.IOException;

/**
 * This is a test class.
 */
public class GridmixTestUtils {
  private static final Log LOG = LogFactory.getLog(GridmixTestUtils.class);
  static final Path DEST = new Path("/gridmix");
  static FileSystem dfs = null;
  static MiniDFSCluster dfsCluster = null;
  static MiniMRClientCluster mrvl = null;
  protected static final String GRIDMIX_USE_QUEUE_IN_TRACE = 
      "gridmix.job-submission.use-queue-in-trace";
  protected static final String GRIDMIX_DEFAULT_QUEUE = 
      "gridmix.job-submission.default-queue";

  public static void initCluster(Class<?> caller) throws IOException {
    Configuration conf = new Configuration();
//    conf.set("mapred.queue.names", "default,q1,q2");
  conf.set("mapred.queue.names", "default");
    conf.set(PREFIX + "root.queues", "default");
    conf.set(PREFIX + "root.default.capacity", "100.0");
    
    
    conf.setBoolean(GRIDMIX_USE_QUEUE_IN_TRACE, false);
    conf.set(GRIDMIX_DEFAULT_QUEUE, "default");
    

    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true)
        .build();// MiniDFSCluster(conf, 3, true, null);
    dfs = dfsCluster.getFileSystem();
    conf.set(JTConfig.JT_RETIREJOBS, "false");
    mrvl = MiniMRClientClusterFactory.create(caller, 2, conf);
    
    conf = mrvl.getConfig();
    String[] files = conf.getStrings(MRJobConfig.CACHE_FILES);
    if (files != null) {
      String[] timestamps = new String[files.length];
      for (int i = 0; i < files.length; i++) {
        timestamps[i] = Long.toString(System.currentTimeMillis());
      }
      conf.setStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS, timestamps);
    }
    
  }

  public static void shutdownCluster() throws IOException {
    if (mrvl != null) {
      mrvl.stop();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  /**
   * Methods to generate the home directory for dummy users.
   * 
   * @param conf
   */
  public static void createHomeAndStagingDirectory(String user,
      Configuration conf) {
    try {
      FileSystem fs = dfsCluster.getFileSystem();
      String path = "/user/" + user;
      Path homeDirectory = new Path(path);
      if (!fs.exists(homeDirectory)) {
        LOG.info("Creating Home directory : " + homeDirectory);
        fs.mkdirs(homeDirectory);
        changePermission(user, homeDirectory, fs);

      }    
      changePermission(user, homeDirectory, fs);
      Path stagingArea = new Path(
          conf.get("mapreduce.jobtracker.staging.root.dir",
              "/tmp/hadoop/mapred/staging"));
      LOG.info("Creating Staging root directory : " + stagingArea);
      fs.mkdirs(stagingArea);
      fs.setPermission(stagingArea, new FsPermission((short) 0777));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  static void changePermission(String user, Path homeDirectory, FileSystem fs)
      throws IOException {
    fs.setOwner(homeDirectory, user, "");
  }
}
