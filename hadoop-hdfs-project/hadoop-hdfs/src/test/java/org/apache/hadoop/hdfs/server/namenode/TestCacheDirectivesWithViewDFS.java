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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.ViewDistributedFileSystem;

import java.io.IOException;
import java.net.URI;

public class TestCacheDirectivesWithViewDFS extends TestCacheDirectives {

  @Override
  public DistributedFileSystem getDFS() throws IOException {
    Configuration conf = getConf();
    conf.set("fs.hdfs.impl", ViewDistributedFileSystem.class.getName());
    URI defaultFSURI =
        URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    ConfigUtil.addLinkFallback(conf, defaultFSURI.getHost(),
        new Path(defaultFSURI.toString()).toUri());
    ConfigUtil.addLink(conf, defaultFSURI.getHost(), "/tmp",
        new Path(defaultFSURI.toString()).toUri());
    return super.getDFS();
  }

  @Override
  public DistributedFileSystem getDFS(MiniDFSCluster cluster, int nnIdx)
      throws IOException {
    Configuration conf = cluster.getConfiguration(nnIdx);
    conf.set("fs.hdfs.impl", ViewDistributedFileSystem.class.getName());
    URI uri = cluster.getURI(0);
    ConfigUtil.addLinkFallback(conf, uri.getHost(), uri);
    ConfigUtil.addLink(conf, uri.getHost(), "/tmp", uri);
    return cluster.getFileSystem(0);
  }
}
