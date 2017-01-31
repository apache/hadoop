/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * Common functionality for S3GuardTool test cases.
 */
public abstract class S3GuardToolTestBase {

  protected static final String OWNER = "hdfs";

  private Configuration conf;
  private MetadataStore ms;
  private S3AFileSystem fs;

  protected Configuration getConf() {
    return conf;
  }

  protected MetadataStore getMetadataStore() {
    return ms;
  }

  protected S3AFileSystem getFs() {
    return fs;
  }

  /** Get test path of s3. */
  protected String getTestPath(String path) {
    return fs.qualify(new Path(path)).toString();
  }

  protected abstract MetadataStore newMetadataStore();

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    fs = S3ATestUtils.createTestFileSystem(conf);

    ms = newMetadataStore();
    ms.initialize(fs);
  }

  @After
  public void tearDown() {
  }

  protected void mkdirs(Path path, boolean onS3, boolean onMetadataStore)
      throws IOException {
    if (onS3) {
      fs.mkdirs(path);
    }
    if (onMetadataStore) {
      S3AFileStatus status = new S3AFileStatus(true, path, OWNER);
      ms.put(new PathMetadata(status));
    }
  }

  protected static void putFile(MetadataStore ms, S3AFileStatus f)
      throws IOException {
    assertNotNull(f);
    ms.put(new PathMetadata(f));
    Path parent = f.getPath().getParent();
    while (parent != null) {
      S3AFileStatus dir = new S3AFileStatus(false, parent, f.getOwner());
      ms.put(new PathMetadata(dir));
      parent = parent.getParent();
    }
  }

  /**
   * Create file either on S3 or in metadata store.
   * @param path the file path.
   * @param onS3 set to true to create the file on S3.
   * @param onMetadataStore set to true to create the file on the
   *                        metadata store.
   * @throws IOException
   */
  protected void createFile(Path path, boolean onS3, boolean onMetadataStore)
      throws IOException {
    if (onS3) {
      ContractTestUtils.touch(fs, path);
    }

    if (onMetadataStore) {
      S3AFileStatus status = new S3AFileStatus(100L, 10000L,
          fs.qualify(path), 512L, "hdfs");
      putFile(ms, status);
    }
  }
}
