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

package org.apache.hadoop.hdfs.web;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestHttpFSPorts {
  private static final Configuration conf = new Configuration();
  
  @Before
  public void setupConfig() {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_KEY, 123);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, 456);    
  }
  
  @Test
  public void testWebHdfsCustomDefaultPorts() throws IOException {
    URI uri = URI.create("webhdfs://localhost");
    WebHdfsFileSystem fs = (WebHdfsFileSystem) FileSystem.get(uri, conf);

    assertEquals(123, fs.getDefaultPort());
    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:123", fs.getCanonicalServiceName());
  }

  @Test
  public void testWebHdfsCustomUriPortWithCustomDefaultPorts() throws IOException {
    URI uri = URI.create("webhdfs://localhost:789");
    WebHdfsFileSystem fs = (WebHdfsFileSystem) FileSystem.get(uri, conf);

    assertEquals(123, fs.getDefaultPort());
    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:789", fs.getCanonicalServiceName());
  }

  @Test
  public void testSWebHdfsCustomDefaultPorts() throws IOException {
    URI uri = URI.create("swebhdfs://localhost");
    SWebHdfsFileSystem fs = (SWebHdfsFileSystem) FileSystem.get(uri, conf);

    assertEquals(456, fs.getDefaultPort());
    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:456", fs.getCanonicalServiceName());
  }
  
  @Test
  public void testSwebHdfsCustomUriPortWithCustomDefaultPorts() throws IOException {
    URI uri = URI.create("swebhdfs://localhost:789");
    SWebHdfsFileSystem fs = (SWebHdfsFileSystem) FileSystem.get(uri, conf);

    assertEquals(456, fs.getDefaultPort());
    assertEquals(uri, fs.getUri());
    assertEquals("127.0.0.1:789", fs.getCanonicalServiceName());
  }
}
