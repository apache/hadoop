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

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestEditLogFileOutputStream {

  @Test
  public void testPreallocation() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
        .build();

    File editLog = cluster.getNameNode().getFSImage().getEditLog()
        .getFsEditName();

    assertEquals("Edit log should only be 4 bytes long",
        4, editLog.length());
    assertEquals("Edit log disk space used should be one block",
        4096, new DU(editLog, conf).getUsed());

    cluster.getFileSystem().mkdirs(new Path("/tmp"),
        new FsPermission((short)777));

    assertEquals("Edit log should be 1MB + 4 bytes long",
        (1024 * 1024) + 4, editLog.length());
    // 256 blocks for the 1MB of preallocation space, 1 block for the original
    // 4 bytes
    assertTrue("Edit log disk space used should be at least 257 blocks",
        257 * 4096 <= new DU(editLog, conf).getUsed());
  }

}
