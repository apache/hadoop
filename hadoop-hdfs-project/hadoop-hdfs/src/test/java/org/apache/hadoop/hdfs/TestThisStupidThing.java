package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URI;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TestThisStupidThing {

  @Rule public TemporaryFolder folder = new TemporaryFolder();
  private MiniDFSCluster cluster;
  private String nameNodeURI;
  public static final int BLOCK_SIZE = 1024;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    File baseDir = folder.newFolder("test").getAbsoluteFile();
    conf.setInt("dfs.blocksize", 1024);
    FileUtil.fullyDelete(baseDir);
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
    cluster = builder.build();
    nameNodeURI = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
  }

  @Test
  public void writingAcrossBlocks() throws Exception {
    FileSystem fileSystem = FileSystem.get(URI.create(nameNodeURI), cluster.getConfiguration(0));
    Path path = new Path("/foo/testAppend.out");
    int numBlocks = 2;
    byte[] data = new byte[BLOCK_SIZE * numBlocks];
    int i = 0;
    FSDataOutputStream stream = fileSystem.append(path, 1024);
    new Random().nextBytes(data);
    stream.write(data);
    stream.close();
  }
}