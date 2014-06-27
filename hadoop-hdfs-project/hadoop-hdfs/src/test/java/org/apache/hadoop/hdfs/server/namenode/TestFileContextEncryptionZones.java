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

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.TestEncryptionZonesAPI;
import org.junit.BeforeClass;

/**
 * Tests of encryption zone operations using FileContext APIs.
 */
public class TestFileContextEncryptionZones extends TestEncryptionZonesAPI  {

  @Override
  protected FileSystem createFileSystem(Configuration conf) throws IOException {
    FileContextFS fcFs = new FileContextFS();
    fcFs.initialize(FileSystem.getDefaultUri(conf), conf);
    return fcFs;
  }

  /**
   * This reuses FSXAttrBaseTest's testcases by creating a filesystem
   * implementation which uses FileContext by only overriding the xattr related
   * methods. Other operations will use the normal filesystem.
   */
  public static class FileContextFS extends DistributedFileSystem {

    private FileContext fc;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      super.initialize(uri, conf);
      fc = FileContext.getFileContext(conf);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      fc.rename(src, dst);
      return true;
    }
  }
}
