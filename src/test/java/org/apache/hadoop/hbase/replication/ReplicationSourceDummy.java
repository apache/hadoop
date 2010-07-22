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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Source that does nothing at all, helpful to test ReplicationSourceManager
 */
public class ReplicationSourceDummy implements ReplicationSourceInterface {

  ReplicationSourceManager manager;
  String peerClusterId;
  Path currentPath;

  @Override
  public void init(Configuration conf, FileSystem fs,
                   ReplicationSourceManager manager, AtomicBoolean stopper,
                   AtomicBoolean replicating, String peerClusterId)
      throws IOException {
    this.manager = manager;
    this.peerClusterId = peerClusterId;
  }

  @Override
  public void enqueueLog(Path log) {
    this.currentPath = log;
  }

  @Override
  public Path getCurrentPath() {
    return this.currentPath;
  }

  @Override
  public void startup() {

  }

  @Override
  public void terminate() {

  }

  @Override
  public String getPeerClusterZnode() {
    return peerClusterId;
  }
}
