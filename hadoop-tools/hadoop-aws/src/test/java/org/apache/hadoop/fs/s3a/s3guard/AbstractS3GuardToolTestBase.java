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

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.SUCCESS;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Common functionality for S3GuardTool test cases.
 */
public abstract class AbstractS3GuardToolTestBase extends AbstractS3ATestBase {

  protected static final String OWNER = "hdfs";
  protected static final String DYNAMODB_TABLE = "dynamodb://ireland-team";
  protected static final String S3A_THIS_BUCKET_DOES_NOT_EXIST
      = "s3a://this-bucket-does-not-exist-00000000000";

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

  /**
   * Run a S3GuardTool command from a varags list.
   * @param conf configuration
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(Configuration conf, String... args)
      throws Exception {
    return S3GuardTool.run(conf, args);
  }

  /**
   * Run a S3GuardTool command from a varags list and the
   * configuration returned by {@code getConfiguration()}.
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(String... args) throws Exception {
    return S3GuardTool.run(getConfiguration(), args);
  }

  /**
   * Run a S3GuardTool command from a varags list, catch any raised
   * ExitException and verify the status code matches that expected.
   * @param status expected status code of the exception
   * @param args argument list
   * @throws Exception any exception
   */
  protected void runToFailure(int status, String... args)
      throws Exception {
    ExitUtil.ExitException ex =
        intercept(ExitUtil.ExitException.class,
            new Callable<Integer>() {
              @Override
              public Integer call() throws Exception {
                return run(args);
              }
            });
    if (ex.status != status) {
      throw ex;
    }
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

      assertMetastoreListingCount(parent, "Children count before pruning", 2);
      exec(cmd, args);
      assertMetastoreListingCount(parent, "Pruned children count", 1);
    } finally {
      getFileSystem().delete(parent, true);
      ms.prune(Long.MAX_VALUE);
    }
  }

  private void assertMetastoreListingCount(Path parent,
      String message,
      int expected) throws IOException {
    Collection<PathMetadata> listing = ms.listChildren(parent).getListing();
    assertEquals(message +" [" + StringUtils.join(", ", listing) + "]",
        expected, listing.size());
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

  @Test
  public void testDestroyNoBucket() throws Throwable {
    intercept(FileNotFoundException.class,
        new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            return run(S3GuardTool.Destroy.NAME,
                S3A_THIS_BUCKET_DOES_NOT_EXIST);
          }
        });
  }

  /**
   * Get the test CSV file; assume() that it is not modified (i.e. we haven't
   * switched to a new storage infrastructure where the bucket is no longer
   * read only).
   * @return test file.
   */
  protected String getLandsatCSVFile() {
    String csvFile = getConfiguration()
        .getTrimmed(KEY_CSVTEST_FILE, DEFAULT_CSVTEST_FILE);
    Assume.assumeTrue("CSV test file is not the default",
        DEFAULT_CSVTEST_FILE.equals(csvFile));
    return csvFile;
  }

  /**
   * Execute a command, returning the buffer if the command actually completes.
   * If an exception is raised the output is logged instead.
   * @param cmd command
   * @param buf buffer to use for tool output (not SLF4J output)
   * @param args argument list
   * @throws Exception on any failure
   */
  public String exec(S3GuardTool cmd, String...args) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try {
      exec(cmd, buf, args);
      return buf.toString();
    } catch (AssertionError e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Command {} failed: \n{}", cmd, buf);
      throw e;
    }
  }

  /**
   * Execute a command, saving the output into the buffer.
   * @param cmd command
   * @param buf buffer to use for tool output (not SLF4J output)
   * @param args argument list
   * @throws Exception on any failure
   */
  protected void exec(S3GuardTool cmd, ByteArrayOutputStream buf, String...args)
      throws Exception {
    LOG.info("exec {}", (Object) args);
    int r = 0;
    try(PrintStream out =new PrintStream(buf)) {
      r = cmd.run(args, out);
      out.flush();
    }
    assertEquals("Command " + cmd + " failed\n"+ buf, 0, r);
  }

}
