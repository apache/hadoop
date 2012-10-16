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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.TestDirHelper;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

@RunWith(value = Parameterized.class)
public class TestHttpFSFileSystemLocalFileSystem extends BaseTestHttpFSWith {

  private static String PATH_PREFIX;

  static {
    new TestDirHelper();
    String prefix =
      System.getProperty("test.build.dir", "target/test-dir") + "/local";
    File file = new File(prefix);
    file.mkdirs();
    PATH_PREFIX = file.getAbsolutePath();
  }

  public TestHttpFSFileSystemLocalFileSystem(Operation operation) {
    super(operation);
  }

  @Override
  protected Path getProxiedFSTestDir() {
    return addPrefix(new Path(TestDirHelper.getTestDir().getAbsolutePath()));
  }

  @Override
  protected String getProxiedFSURI() {
    return "file:///";
  }

  @Override
  protected Configuration getProxiedFSConf() {
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, getProxiedFSURI());
    return conf;
  }

  protected Path addPrefix(Path path) {
    URI uri = path.toUri();
    try {
      if (uri.getAuthority() != null) {
        uri = new URI(uri.getScheme(),
                      uri.getAuthority(), PATH_PREFIX + uri.getPath());
      }
      else {
        if (uri.getPath().startsWith("/")) {
          uri = new URI(PATH_PREFIX + uri.getPath());
        }
      }
    } catch (URISyntaxException ex) {
      throw new RuntimeException("It should not happen: " + ex.toString(), ex);
    }
    return new Path(uri);
  }

}
