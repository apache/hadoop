/*
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.SUCCESS;

/**
 * Common functionality for S3GuardTool test cases.
 */
public abstract class AbstractS3GuardToolTestBase extends AbstractS3ATestBase {

  protected static final String OWNER = "hdfs";

  private MetadataStore ms;

  protected static void expectResult(int expected,
      String message,
      S3GuardTool tool,
      String... args) throws Exception {
    assertEquals(message, expected, tool.run(args));
  }

  protected static void expectSuccess(
      String message,
      S3GuardTool tool,
      String... args) throws Exception {
    assertEquals(message, SUCCESS, tool.run(args));
  }

  protected MetadataStore getMetadataStore() {
    return ms;
  }

  protected abstract MetadataStore newMetadataStore();

  @Override
  public void setup() throws Exception {
    super.setup();
    S3ATestUtils.assumeS3GuardState(true, getConfiguration());
    ms = newMetadataStore();
    ms.initialize(getFileSystem());
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    IOUtils.cleanupWithLogger(LOG, ms);
  }

  protected void mkdirs(Path path, boolean onS3, boolean onMetadataStore)
      throws IOException {
    if (onS3) {
      getFileSystem().mkdirs(path);
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
   * @throws IOException IO problem
   */
  protected void createFile(Path path, boolean onS3, boolean onMetadataStore)
      throws IOException {
    if (onS3) {
      ContractTestUtils.touch(getFileSystem(), path);
    }

    if (onMetadataStore) {
      S3AFileStatus status = new S3AFileStatus(100L, System.currentTimeMillis(),
          getFileSystem().qualify(path), 512L, "hdfs");
      putFile(ms, status);
    }
  }

  private void testPruneCommand(Configuration cmdConf, String...args)
      throws Exception {
    Path parent = path("prune-cli");
    try {
      getFileSystem().mkdirs(parent);

      S3GuardTool.Prune cmd = new S3GuardTool.Prune(cmdConf);
      cmd.setMetadataStore(ms);

      createFile(new Path(parent, "stale"), true, true);
      Thread.sleep(TimeUnit.SECONDS.toMillis(2));
      createFile(new Path(parent, "fresh"), true, true);

      assertEquals(2, ms.listChildren(parent).getListing().size());
      expectSuccess("Prune command did not exit successfully - see output", cmd,
          args);
      assertEquals(1, ms.listChildren(parent).getListing().size());
    } finally {
      getFileSystem().delete(parent, true);
      ms.prune(Long.MAX_VALUE);
    }
  }

  @Test
  public void testPruneCommandCLI() throws Exception {
    String testPath = path("testPruneCommandCLI").toString();
    testPruneCommand(getFileSystem().getConf(),
        "prune", "-seconds", "1", testPath);
  }

  @Test
  public void testPruneCommandConf() throws Exception {
    getConfiguration().setLong(Constants.S3GUARD_CLI_PRUNE_AGE,
        TimeUnit.SECONDS.toMillis(1));
    String testPath = path("testPruneCommandConf").toString();
    testPruneCommand(getConfiguration(), "prune", testPath);
  }
}
