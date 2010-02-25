/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.sun.jersey.spi.container.servlet.ServletContainer;

import junit.framework.TestCase;

public class MiniClusterTestBase extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(MiniClusterTestBase.class);

  public static final String MIMETYPE_BINARY = "application/octet-stream";
  public static final String MIMETYPE_JSON = "application/json";
  public static final String MIMETYPE_PLAIN = "text/plain";
  public static final String MIMETYPE_PROTOBUF = "application/x-protobuf";
  public static final String MIMETYPE_XML = "text/xml";

  // use a nonstandard port
  public static final int DEFAULT_TEST_PORT = 38080;

  protected static Configuration conf = HBaseConfiguration.create();
  protected static MiniZooKeeperCluster zooKeeperCluster;
  protected static MiniHBaseCluster hbaseCluster;
  protected static MiniDFSCluster dfsCluster;
  protected static File testDir;
  protected static int testServletPort;
  protected static Server server;

  public static boolean isMiniClusterRunning() {
    return server != null;
  }

  private static void startDFS() throws Exception {
    if (dfsCluster != null) {
      LOG.error("MiniDFSCluster already running");
      return;
    }
    // This spews a bunch of warnings about missing scheme. TODO: fix.
    dfsCluster = new MiniDFSCluster(0, conf, 2, true, true, true,
      null, null, null, null);
    // mangle the conf so that the fs parameter points to the minidfs we
    // just started up
    FileSystem filesystem = dfsCluster.getFileSystem();
    conf.set("fs.defaultFS", filesystem.getUri().toString());
    Path parentdir = filesystem.getHomeDirectory();
    conf.set(HConstants.HBASE_DIR, parentdir.toString());
    filesystem.mkdirs(parentdir);
    FSUtils.setVersion(filesystem, parentdir);
  }

  private static void stopDFS() {
    if (dfsCluster != null) try {
      FileSystem fs = dfsCluster.getFileSystem();
      if (fs != null) {
        LOG.info("Shutting down FileSystem");
        fs.close();
      }
      FileSystem.closeAll();
      dfsCluster = null;
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }

  private static void startZooKeeper() throws Exception {
    if (zooKeeperCluster != null) {
      LOG.error("ZooKeeper already running");
      return;
    }
    zooKeeperCluster = new MiniZooKeeperCluster();
    zooKeeperCluster.startup(testDir);
    LOG.info("started " + zooKeeperCluster.getClass().getName());
  }

  private static void stopZooKeeper() {
    if (zooKeeperCluster != null) try {
      zooKeeperCluster.shutdown();
      zooKeeperCluster = null;
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }
  
  private static void startHBase() throws Exception {
    if (hbaseCluster != null) {
      LOG.error("MiniHBaseCluster already running");
      return;
    }
    hbaseCluster = new MiniHBaseCluster(conf, 1);
    // opening the META table ensures that cluster is running
    new HTable(conf, HConstants.META_TABLE_NAME);
    LOG.info("started MiniHBaseCluster");
  }
  
  private static void stopHBase() {
    if (hbaseCluster != null) try {
      HConnectionManager.deleteConnectionInfo(conf, true);
      hbaseCluster.shutdown();
      hbaseCluster = null;
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }

  private static void startServletContainer() throws Exception {
    if (server != null) {
      LOG.error("ServletContainer already running");
      return;
    }

    // set up the Jersey servlet container for Jetty
    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter(
      "com.sun.jersey.config.property.resourceConfigClass",
      ResourceConfig.class.getCanonicalName());
    sh.setInitParameter("com.sun.jersey.config.property.packages",
      "jetty");

    LOG.info("configured " + ServletContainer.class.getName());
    
    // set up Jetty and run the embedded server
    testServletPort = conf.getInt("test.stargate.port", DEFAULT_TEST_PORT);
    server = new Server(testServletPort);
    server.setSendServerVersion(false);
    server.setSendDateHeader(false);
      // set up context
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addServlet(sh, "/*");
      // start the server
    server.start();

    LOG.info("started " + server.getClass().getName() + " on port " + 
      testServletPort);
  }

  private static void stopServletContainer() {
    if (server != null) try {
      server.stop();
      server = null;
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }

  public static void startMiniCluster() throws Exception {
    try {
      startDFS();
      startZooKeeper();
      startHBase();
      startServletContainer();
    } catch (Exception e) {
      stopServletContainer();
      stopHBase();
      stopZooKeeper();
      stopDFS();
      throw e;
    }
  }

  public static void stopMiniCluster() {
    stopServletContainer();
    stopHBase();
    stopZooKeeper();
    stopDFS();
  }

  static class MiniClusterShutdownThread extends Thread {
    public void run() {
      stopMiniCluster();
      Path path = new Path(
        conf.get("test.build.data",
          System.getProperty("test.build.data", "build/test/data")));
      try {
        FileSystem.get(conf).delete(path, true);
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
      }
    }
  }

  @Override
  protected void setUp() throws Exception {
    // start the mini cluster if it is not running yet
    if (!isMiniClusterRunning()) {
      startMiniCluster();
      Runtime.getRuntime().addShutdownHook(new MiniClusterShutdownThread());
    }

    // tell HttpClient to dump request and response headers into the test
    // log at DEBUG level
    Logger.getLogger("httpclient.wire.header").setLevel(Level.DEBUG);

    super.setUp();
  }
}
