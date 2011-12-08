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

package org.apache.hadoop.fs.http.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.test.TestJettyHelper;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;

@RunWith(value = Parameterized.class)
public class TestWebhdfsFileSystem extends TestHttpFSFileSystem {

  public TestWebhdfsFileSystem(TestHttpFSFileSystem.Operation operation) {
    super(operation);
  }

  @Override
  protected FileSystem getHttpFileSystem() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.webhdfs.impl", WebHdfsFileSystem.class.getName());
    URI uri = new URI("webhdfs://" + TestJettyHelper.getJettyURL().toURI().getAuthority());
    return FileSystem.get(uri, conf);
  }

  @Override
  protected void testGet() throws Exception {
    FileSystem fs = getHttpFileSystem();
    Assert.assertNotNull(fs);
    URI uri = new URI("webhdfs://" + TestJettyHelper.getJettyURL().toURI().getAuthority());
    Assert.assertEquals(fs.getUri(), uri);
    fs.close();
  }

}
