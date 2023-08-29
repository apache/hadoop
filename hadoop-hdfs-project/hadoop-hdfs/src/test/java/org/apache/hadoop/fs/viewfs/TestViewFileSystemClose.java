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
package org.apache.hadoop.fs.viewfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.junit.Test;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestViewFileSystemClose extends AbstractHadoopTestBase {

  /**
   * Verify that all child file systems of a ViewFileSystem will be shut down
   * when the cache is disabled.
   * @throws IOException
   */
  @Test
  public void testFileSystemLeak() throws Exception {

    Configuration conf = new Configuration();
    conf.set("fs.viewfs.impl", ViewFileSystem.class.getName());
    conf.setBoolean("fs.viewfs.enable.inner.cache", false);
    conf.setBoolean("fs.viewfs.impl.disable.cache", true);
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    String rootPath = "hdfs://localhost/tmp";
    ConfigUtil.addLink(conf, "/data", new Path(rootPath, "data").toUri());
    ViewFileSystem viewFs =
        (ViewFileSystem) FileSystem.get(FsConstants.VIEWFS_URI, conf);

    FileSystem[] children = viewFs.getChildFileSystems();
    viewFs.close();
    FileSystem.closeAll();
    for (FileSystem fs : children) {
      intercept(IOException.class, "Filesystem closed",
          "Expect Filesystem closed IOException",
          () -> fs.create(new Path(rootPath, "neverSuccess")));
    }
  }
}
