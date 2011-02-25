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

package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;


public class TestDFSUtil {
  
  @Test
  public void testMultipleNamenodes() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_FEDERATION_NAMENODES, "nn1,nn2");
    conf.set("sn1-primary.namenode", "nn1");
    conf.set("sn2-primary.namenode", "nn2");
    List<URI> namenodes = DFSUtil.getNamenodeList(conf);
    Iterator<URI> it = namenodes.iterator();
    assertEquals("nn1", it.next().toString());
    assertEquals("nn2", it.next().toString());
    assertEquals("nn1", DFSUtil.getPrimaryNamenode(conf, "sn1").toString());
    assertEquals("nn2", DFSUtil.getPrimaryNamenode(conf, "sn2").toString());
  }
  
  @Test
  public void testDefaultNamenode() {
    HdfsConfiguration conf = new HdfsConfiguration();
    final String hdfs_default = "hdfs://x.y.z:9999/";
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, hdfs_default);
    List<URI> namenodes = DFSUtil.getNamenodeList(conf);
    assertEquals(1, namenodes.size());
    Iterator<URI> it = namenodes.iterator();
    assertEquals(hdfs_default, it.next().toString());
    assertEquals(hdfs_default, DFSUtil.getPrimaryNamenode(conf, "sn1")
        .toString());
  }
}
