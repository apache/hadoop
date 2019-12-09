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
package org.apache.hadoop.fs;

import java.net.URI;

import org.apache.commons.net.ftp.FTP;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestDelegateToFileSystem {

  private static final String FTP_DUMMYHOST = "ftp://dummyhost";
  private static final URI FTP_URI_NO_PORT = URI.create(FTP_DUMMYHOST);
  private static final URI FTP_URI_WITH_PORT = URI.create(FTP_DUMMYHOST + ":"
      + FTP.DEFAULT_PORT);

  private void testDefaultUriInternal(String defaultUri)
      throws UnsupportedFileSystemException {
    final Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, defaultUri);
    final AbstractFileSystem ftpFs =
        AbstractFileSystem.get(FTP_URI_NO_PORT, conf);
    Assert.assertEquals(FTP_URI_WITH_PORT, ftpFs.getUri());
  }

  @Test
  public void testDefaultURIwithOutPort() throws Exception {
    testDefaultUriInternal("hdfs://dummyhost");
  }

  @Test
  public void testDefaultURIwithPort() throws Exception {
    testDefaultUriInternal("hdfs://dummyhost:8020");
  }
}
