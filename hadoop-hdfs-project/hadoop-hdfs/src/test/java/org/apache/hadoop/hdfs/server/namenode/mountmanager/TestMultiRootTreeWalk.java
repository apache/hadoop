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
package org.apache.hadoop.hdfs.server.namenode.mountmanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link FSMultiRootTreeWalk}.
 */
public class TestMultiRootTreeWalk {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestMultiRootTreeWalk.class);

  private final File fBase = new File(MiniDFSCluster.getBaseDirectory());
  private final Path base = new Path(fBase.toURI().toString());
  private final Path providedPath = new Path(base, "providedDir");
  private Configuration conf;

  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
  }

  @Test
  public void testExplorePaths() throws Exception {
    Map<Path, FSMultiRootTreeWalk.Node> pathsExplored = new HashMap<>();

    File d1 = new File(new Path(providedPath, "dir1").toUri());
    File d2 = new File(new Path(providedPath, "dir1").toUri());
    d1.mkdirs();
    d2.mkdirs();
    FSMultiRootTreeWalk.Node root = new FSMultiRootTreeWalk.Node(new Path("/"));
    FSTreeWalk remoteTreeWalk1 = new FSTreeWalk(new Path(d1.toURI()), conf);
    FSTreeWalk remoteTreeWalk2 = new FSTreeWalk(new Path(d2.toURI()), conf);
    FSMultiRootTreeWalk.explorePath(root, new Path("/user/data/d/"),
        remoteTreeWalk1, pathsExplored);
    FSMultiRootTreeWalk.explorePath(root, new Path("/user/data/e/"),
        remoteTreeWalk2, pathsExplored);

    assertEquals(4, pathsExplored.size());
    assertEquals(1, root.getChildren().size());
    assertEquals(new Path("/user"),
        root.getChildren().iterator().next().getPath());
    assertEquals(new Path("/user/data/e/"),
        pathsExplored.get(new Path("/user/data/e/")).getPath());
  }
}