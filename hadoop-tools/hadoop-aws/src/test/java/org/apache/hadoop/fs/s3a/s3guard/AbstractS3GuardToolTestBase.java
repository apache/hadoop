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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.util.StopWatch;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_NAME_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_METASTORE_NULL;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.E_BAD_STATE;
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

  private static final int PRUNE_MAX_AGE_SECS = 2;

  private MetadataStore ms;
  private S3AFileSystem rawFs;

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

  @Override
  public void setup() throws Exception {
    super.setup();
    S3ATestUtils.assumeS3GuardState(true, getConfiguration());
    ms = getFileSystem().getMetadataStore();

    // Also create a "raw" fs without any MetadataStore configured
    Configuration conf = new Configuration(getConfiguration());
    conf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NULL);
    URI fsUri = getFileSystem().getUri();
    rawFs = (S3AFileSystem) FileSystem.newInstance(fsUri, conf);
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    IOUtils.cleanupWithLogger(LOG, ms);
    IOUtils.closeStream(rawFs);
  }

  protected void mkdirs(Path path, boolean onS3, boolean onMetadataStore)
      throws IOException {
    Preconditions.checkArgument(onS3 || onMetadataStore);
    // getFileSystem() returns an fs with MetadataStore configured
    S3AFileSystem fs = onMetadataStore ? getFileSystem() : rawFs;
    if (onS3) {
      fs.mkdirs(path);
    } else if (onMetadataStore) {
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
    Preconditions.checkArgument(onS3 || onMetadataStore);
    // getFileSystem() returns an fs with MetadataStore configured
    S3AFileSystem fs = onMetadataStore ? getFileSystem() : rawFs;
    if (onS3) {
      ContractTestUtils.touch(fs, path);
    } else if (onMetadataStore) {
      S3AFileStatus status = new S3AFileStatus(100L, System.currentTimeMillis(),
          fs.qualify(path), 512L, "hdfs");
      putFile(ms, status);
    }
  }

  /**
   * Attempt to test prune() with sleep() without having flaky tests
   * when things run slowly. Test is basically:
   * 1. Set max path age to X seconds
   * 2. Create some files (which writes entries to MetadataStore)
   * 3. Sleep X+2 seconds (all files from above are now "stale")
   * 4. Create some other files (these are "fresh").
   * 5. Run prune on MetadataStore.
   * 6. Assert that only files that were created before the sleep() were pruned.
   *
   * Problem is: #6 can fail if X seconds elapse between steps 4 and 5, since
   * the newer files also become stale and get pruned.  This is easy to
   * reproduce by running all integration tests in parallel with a ton of
   * threads, or anything else that slows down execution a lot.
   *
   * Solution: Keep track of time elapsed between #4 and #5, and if it
   * exceeds X, just print a warn() message instead of failing.
   *
   * @param cmdConf configuration for command
   * @param parent path
   * @param args command args
   * @throws Exception
   */
  private void testPruneCommand(Configuration cmdConf, Path parent,
      String...args) throws Exception {
    Path keepParent = path("prune-cli-keep");
    StopWatch timer = new StopWatch();
    try {
      S3GuardTool.Prune cmd = new S3GuardTool.Prune(cmdConf);
      cmd.setMetadataStore(ms);

      getFileSystem().mkdirs(parent);
      getFileSystem().mkdirs(keepParent);
      createFile(new Path(parent, "stale"), true, true);
      createFile(new Path(keepParent, "stale-to-keep"), true, true);

      Thread.sleep(TimeUnit.SECONDS.toMillis(PRUNE_MAX_AGE_SECS + 2));

      timer.start();
      createFile(new Path(parent, "fresh"), true, true);

      assertMetastoreListingCount(parent, "Children count before pruning", 2);
      exec(cmd, args);
      long msecElapsed = timer.now(TimeUnit.MILLISECONDS);
      if (msecElapsed >= PRUNE_MAX_AGE_SECS * 1000) {
        LOG.warn("Skipping an assertion: Test running too slowly ({} msec)",
            msecElapsed);
      } else {
        assertMetastoreListingCount(parent, "Pruned children count remaining",
            1);
      }
      assertMetastoreListingCount(keepParent,
          "This child should have been kept (prefix restriction).", 1);
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
    Path testPath = path("testPruneCommandCLI");
    testPruneCommand(getFileSystem().getConf(), testPath,
        "prune", "-seconds", String.valueOf(PRUNE_MAX_AGE_SECS),
        testPath.toString());
  }

  @Test
  public void testPruneCommandConf() throws Exception {
    getConfiguration().setLong(Constants.S3GUARD_CLI_PRUNE_AGE,
        TimeUnit.SECONDS.toMillis(PRUNE_MAX_AGE_SECS));
    Path testPath = path("testPruneCommandConf");
    testPruneCommand(getConfiguration(), testPath,
        "prune", testPath.toString());
  }

  @Test
  public void testSetCapacityFailFast() throws Exception{
    Configuration conf = getConfiguration();
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, getFileSystem().getBucket());

    S3GuardTool.SetCapacity cmdR = new S3GuardTool.SetCapacity(conf);
    String[] argsR = new String[]{cmdR.getName(), "-read", "0", "s3a://bucket"};
    intercept(IllegalArgumentException.class,
        S3GuardTool.SetCapacity.READ_CAP_INVALID, () -> cmdR.run(argsR));

    S3GuardTool.SetCapacity cmdW = new S3GuardTool.SetCapacity(conf);
    String[] argsW = new String[]{cmdW.getName(), "-write", "0",
        "s3a://bucket"};
    intercept(IllegalArgumentException.class,
        S3GuardTool.SetCapacity.WRITE_CAP_INVALID, () -> cmdW.run(argsW));
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

  @Test
  public void testProbeForMagic() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    String name = fs.getUri().toString();
    S3GuardTool.BucketInfo cmd = new S3GuardTool.BucketInfo(
        getConfiguration());
    if (fs.hasCapability(
        CommitConstants.STORE_CAPABILITY_MAGIC_COMMITTER)) {
      // if the FS is magic, expect this to work
      exec(cmd, S3GuardTool.BucketInfo.MAGIC_FLAG, name);
    } else {
      // if the FS isn't magic, expect the probe to fail
      ExitUtil.ExitException e = intercept(ExitUtil.ExitException.class,
          () -> exec(cmd, S3GuardTool.BucketInfo.MAGIC_FLAG, name));
      if (e.getExitCode() != E_BAD_STATE) {
        throw e;
      }
    }
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

  @Test
  public void testDiffCommand() throws Exception {
    S3AFileSystem fs = getFileSystem();
    ms = getMetadataStore();
    Set<Path> filesOnS3 = new HashSet<>(); // files on S3.
    Set<Path> filesOnMS = new HashSet<>(); // files on metadata store.

    Path testPath = path("test-diff");
    mkdirs(testPath, true, true);

    Path msOnlyPath = new Path(testPath, "ms_only");
    mkdirs(msOnlyPath, false, true);
    filesOnMS.add(msOnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(msOnlyPath, String.format("file-%d", i));
      createFile(file, false, true);
      filesOnMS.add(file);
    }

    Path s3OnlyPath = new Path(testPath, "s3_only");
    mkdirs(s3OnlyPath, true, false);
    filesOnS3.add(s3OnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(s3OnlyPath, String.format("file-%d", i));
      createFile(file, true, false);
      filesOnS3.add(file);
    }

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    S3GuardTool.Diff cmd = new S3GuardTool.Diff(fs.getConf());
    cmd.setStore(ms);
    exec(cmd, buf, "diff", "-meta", DYNAMODB_TABLE, testPath.toString());

    Set<Path> actualOnS3 = new HashSet<>();
    Set<Path> actualOnMS = new HashSet<>();
    boolean duplicates = false;
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(
            new ByteArrayInputStream(buf.toByteArray())))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] fields = line.split("\\s");
        assertEquals("[" + line + "] does not have enough fields",
            4, fields.length);
        String where = fields[0];
        Path path = new Path(fields[3]);
        if (S3GuardTool.Diff.S3_PREFIX.equals(where)) {
          duplicates = duplicates || actualOnS3.contains(path);
          actualOnS3.add(path);
        } else if (S3GuardTool.Diff.MS_PREFIX.equals(where)) {
          duplicates = duplicates || actualOnMS.contains(path);
          actualOnMS.add(path);
        } else {
          fail("Unknown prefix: " + where);
        }
      }
    }
    String actualOut = buf.toString();
    assertEquals("Mismatched metadata store outputs: " + actualOut,
        filesOnMS, actualOnMS);
    assertEquals("Mismatched s3 outputs: " + actualOut, filesOnS3, actualOnS3);
    assertFalse("Diff contained duplicates", duplicates);
  }
}
