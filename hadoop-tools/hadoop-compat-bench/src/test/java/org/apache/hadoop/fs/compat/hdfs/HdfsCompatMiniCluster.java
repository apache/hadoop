/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;


public class HdfsCompatMiniCluster {
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsCompatMiniCluster.class);

  private MiniDFSCluster cluster = null;

  public HdfsCompatMiniCluster() {
  }

  public synchronized void start() throws IOException {
    FileSystem.enableSymlinks();
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY, "true");
    conf.set(DFSConfigKeys.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        "kms://http@localhost:9600/kms/foo");
    conf.set(DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_MODE_KEY, "external");
    conf.set(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, "true");
    conf.set("fs.hdfs.compatibility.privileged.user",
        UserGroupInformation.getCurrentUser().getShortUserName());
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitClusterUp();
  }

  public synchronized void shutdown() {
    if (cluster != null) {
      cluster.shutdown(true);
      cluster = null;
    }
  }

  public synchronized Configuration getConf() throws IOException {
    if (cluster == null) {
      throw new IOException("Cluster not running");
    }
    return cluster.getFileSystem().getConf();
  }

  public synchronized URI getUri() throws IOException {
    if (cluster == null) {
      throw new IOException("Cluster not running");
    }
    return cluster.getFileSystem().getUri();
  }

  public static void main(String[] args)
      throws IOException, InterruptedException {
    long duration = 5L * 60L * 1000L;
    if ((args != null) && (args.length > 0)) {
      duration = Long.parseLong(args[0]);
    }

    HdfsCompatMiniCluster cluster = new HdfsCompatMiniCluster();
    try {
      cluster.start();
      Configuration conf = cluster.getConf();

      final String confDir = System.getenv("HADOOP_CONF_DIR");
      final File confFile = new File(confDir, "core-site.xml");
      try (OutputStream out = new FileOutputStream(confFile)) {
        conf.writeXml(out);
      }

      final long endTime = System.currentTimeMillis() + duration;
      long sleepTime = getSleepTime(endTime);
      while (sleepTime > 0) {
        LOG.warn("Service running ...");
        Thread.sleep(sleepTime);
        sleepTime = getSleepTime(endTime);
      }
    } finally {
      cluster.shutdown();
    }
  }

  private static long getSleepTime(long endTime) {
    long maxTime = endTime - System.currentTimeMillis();
    return (maxTime < 5000) ? maxTime : 5000;
  }
}