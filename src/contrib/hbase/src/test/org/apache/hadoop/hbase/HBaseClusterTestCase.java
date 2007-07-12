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

/**
 * Abstract base class for HBase cluster junit tests.  Spins up cluster on
 * {@link #setUp()} and takes it down again in {@link #tearDown()}.
 */
public abstract class HBaseClusterTestCase extends HBaseTestCase {
  protected MiniHBaseCluster cluster;
  final boolean miniHdfs;
  int regionServers;
  
  protected HBaseClusterTestCase() {
    this(true);
  }
  
  protected HBaseClusterTestCase(int regionServers) {
    this(true);
    this.regionServers = regionServers;
  }

  protected HBaseClusterTestCase(String name) {
    this(name, true);
  }
  
  protected HBaseClusterTestCase(final boolean miniHdfs) {
    super();
    this.miniHdfs = miniHdfs;
    this.regionServers = 1;
  }

  protected HBaseClusterTestCase(String name, final boolean miniHdfs) {
    super(name);
    this.miniHdfs = miniHdfs;
    this.regionServers = 1;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.cluster =
      new MiniHBaseCluster(this.conf, this.regionServers, this.miniHdfs);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.cluster != null) {
      this.cluster.shutdown();
    }
  }
}
