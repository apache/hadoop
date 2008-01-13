/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Abstract base class for HBase cluster junit tests.  Spins up an hbase
 * cluster in setup and tears it down again in tearDown.
 */
public abstract class HBaseClusterTestCase extends HBaseTestCase {
  private static final Log LOG =
    LogFactory.getLog(HBaseClusterTestCase.class.getName());
  
  protected MiniHBaseCluster cluster;
  final boolean miniHdfs;
  int regionServers;
  
  /**
   * constructor
   */
  public HBaseClusterTestCase() {
    this(true);
  }
  
  /**
   * @param regionServers
   */
  public HBaseClusterTestCase(int regionServers) {
    this(true);
    this.regionServers = regionServers;
  }

  /**
   * @param name
   */
  public HBaseClusterTestCase(String name) {
    this(name, true);
  }
  
  /**
   * @param miniHdfs
   */
  public HBaseClusterTestCase(final boolean miniHdfs) {
    super();
    this.miniHdfs = miniHdfs;
    this.regionServers = 1;
  }

  /**
   * @param name
   * @param miniHdfs
   */
  public HBaseClusterTestCase(String name, final boolean miniHdfs) {
    super(name);
    this.miniHdfs = miniHdfs;
    this.regionServers = 1;
  }

  @Override
  protected void setUp() throws Exception {
    this.cluster =
      new MiniHBaseCluster(this.conf, this.regionServers, this.miniHdfs);
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    HConnectionManager.deleteConnection(conf);
    if (this.cluster != null) {
      try {
        this.cluster.shutdown();
      } catch (Exception e) {
        LOG.warn("Closing mini dfs", e);
      }
    }
    // ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
    //  "Temporary end-of-test thread dump debugging HADOOP-2040: " + getName());
  }

  
  /**
   * Use this utility method debugging why cluster won't go down.  On a
   * period it throws a thread dump.  Method ends when all cluster
   * regionservers and master threads are no long alive.
   */
  public void threadDumpingJoin() {
    if (this.cluster.getRegionThreads() != null) {
      for(Thread t: this.cluster.getRegionThreads()) {
        threadDumpingJoin(t);
      }
    }
    threadDumpingJoin(this.cluster.getMaster());
  }

  protected void threadDumpingJoin(final Thread t) {
    if (t == null) {
      return;
    }
    long startTime = System.currentTimeMillis();
    while (t.isAlive()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.info("Continuing...", e);
      }
      if (System.currentTimeMillis() - startTime > 60000) {
        startTime = System.currentTimeMillis();
        ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
            "Automatic Stack Trace every 60 seconds waiting on " +
            t.getName());
      }
    }
  }
}
