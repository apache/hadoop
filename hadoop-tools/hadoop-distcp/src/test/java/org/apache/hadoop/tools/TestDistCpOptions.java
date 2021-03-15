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

package org.apache.hadoop.tools;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.tools.DistCpOptions.MAX_NUM_LISTSTATUS_THREADS;
import static org.junit.Assert.fail;

/**
 * This is to test constructing {@link DistCpOptions} manually with setters.
 *
 * The test cases in this class is very similar to the parser test, see
 * {@link TestOptionsParser}.
 */
public class TestDistCpOptions {

  private static final float DELTA = 0.001f;

  @Test
  public void testSetIgnoreFailure() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(builder.build().shouldIgnoreFailures());

    builder.withIgnoreFailures(true);
    Assert.assertTrue(builder.build().shouldIgnoreFailures());
  }

  @Test
  public void testSetOverwrite() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(builder.build().shouldOverwrite());

    builder.withOverwrite(true);
    Assert.assertTrue(builder.build().shouldOverwrite());

    try {
      builder.withSyncFolder(true).build();
      Assert.fail("Update and overwrite aren't allowed together");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testLogPath() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(builder.build().getLogPath());

    final Path logPath = new Path("hdfs://localhost:8020/logs");
    builder.withLogPath(logPath);
    Assert.assertEquals(logPath, builder.build().getLogPath());
  }

  @Test
  public void testSetBlokcing() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertTrue(builder.build().shouldBlock());

    builder.withBlocking(false);
    Assert.assertFalse(builder.build().shouldBlock());
  }

  @Test
  public void testSetBandwidth() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertEquals(0, builder.build().getMapBandwidth(), DELTA);

    builder.withMapBandwidth(11);
    Assert.assertEquals(11, builder.build().getMapBandwidth(), DELTA);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetNonPositiveBandwidth() {
    new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"))
        .withMapBandwidth(-11)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetZeroBandwidth() {
    new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"))
        .withMapBandwidth(0)
        .build();
  }

  @Test
  public void testSetSkipCRC() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(builder.build().shouldSkipCRC());

    final DistCpOptions options = builder.withSyncFolder(true).withCRC(true)
        .build();
    Assert.assertTrue(options.shouldSyncFolder());
    Assert.assertTrue(options.shouldSkipCRC());
  }

  @Test
  public void testSetAtomicCommit() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(builder.build().shouldAtomicCommit());

    builder.withAtomicCommit(true);
    Assert.assertTrue(builder.build().shouldAtomicCommit());

    try {
      builder.withSyncFolder(true).build();
      Assert.fail("Atomic and sync folders were mutually exclusive");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testSetWorkPath() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(builder.build().getAtomicWorkPath());

    builder.withAtomicCommit(true);
    Assert.assertNull(builder.build().getAtomicWorkPath());

    final Path workPath = new Path("hdfs://localhost:8020/work");
    builder.withAtomicWorkPath(workPath);
    Assert.assertEquals(workPath, builder.build().getAtomicWorkPath());
  }

  @Test
  public void testSetSyncFolders() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(builder.build().shouldSyncFolder());

    builder.withSyncFolder(true);
    Assert.assertTrue(builder.build().shouldSyncFolder());
  }

  @Test
  public void testSetDeleteMissing() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(builder.build().shouldDeleteMissing());

    DistCpOptions options = builder.withSyncFolder(true)
        .withDeleteMissing(true)
        .build();
    Assert.assertTrue(options.shouldSyncFolder());
    Assert.assertTrue(options.shouldDeleteMissing());

    options = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"))
        .withOverwrite(true)
        .withDeleteMissing(true)
        .build();
    Assert.assertTrue(options.shouldOverwrite());
    Assert.assertTrue(options.shouldDeleteMissing());

    try {
      new DistCpOptions.Builder(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"))
          .withDeleteMissing(true)
          .build();
      fail("Delete missing should fail without update or overwrite options");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("Delete missing is applicable only with update " +
          "or overwrite options", e);
    }
    try {
      new DistCpOptions.Builder(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"))
          .withSyncFolder(true)
          .withDeleteMissing(true)
          .withUseDiff("s1", "s2")
          .build();
      fail("Should have failed as -delete and -diff are mutually exclusive.");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-delete and -diff/-rdiff are mutually exclusive.", e);
    }
  }

  @Test
  public void testSetMaps() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertEquals(DistCpConstants.DEFAULT_MAPS,
        builder.build().getMaxMaps());

    builder.maxMaps(1);
    Assert.assertEquals(1, builder.build().getMaxMaps());

    builder.maxMaps(0);
    Assert.assertEquals(1, builder.build().getMaxMaps());
  }

  @Test
  public void testSetNumListtatusThreads() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    // If command line argument isn't set, we expect .getNumListstatusThreads
    // option to be zero (so that we know when to override conf properties).
    Assert.assertEquals(0, builder.build().getNumListstatusThreads());

    builder.withNumListstatusThreads(12);
    Assert.assertEquals(12, builder.build().getNumListstatusThreads());

    builder.withNumListstatusThreads(0);
    Assert.assertEquals(0, builder.build().getNumListstatusThreads());

    // Ignore large number of threads.
    builder.withNumListstatusThreads(MAX_NUM_LISTSTATUS_THREADS * 2);
    Assert.assertEquals(MAX_NUM_LISTSTATUS_THREADS,
        builder.build().getNumListstatusThreads());
  }

  @Test
  public void testSourceListing() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertEquals(new Path("hdfs://localhost:8020/source/first"),
        builder.build().getSourceFileListing());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingTarget() {
    new DistCpOptions.Builder(new Path("hdfs://localhost:8020/source/first"),
        null);
  }

  @Test
  public void testToString() {
    DistCpOptions option = new DistCpOptions.Builder(new Path("abc"),
        new Path("xyz")).build();
    String val = "DistCpOptions{atomicCommit=false, syncFolder=false, " +
        "deleteMissing=false, ignoreFailures=false, overwrite=false, " +
        "append=false, useDiff=false, useRdiff=false, " +
        "fromSnapshot=null, toSnapshot=null, " +
        "skipCRC=false, blocking=true, numListstatusThreads=0, maxMaps=20, " +
        "mapBandwidth=0.0, copyStrategy='uniformsize', preserveStatus=[], " +
        "atomicWorkPath=null, logPath=null, sourceFileListing=abc, " +
        "sourcePaths=null, targetPath=xyz, filtersFile='null', " +
        "blocksPerChunk=0, copyBufferSize=8192, verboseLog=false, " +
        "directWrite=false, useiterator=false}";
    String optionString = option.toString();
    Assert.assertEquals(val, optionString);
    Assert.assertNotSame(DistCpOptionSwitch.ATOMIC_COMMIT.toString(),
        DistCpOptionSwitch.ATOMIC_COMMIT.name());
  }

  @Test
  public void testCopyStrategy() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertEquals(DistCpConstants.UNIFORMSIZE,
        builder.build().getCopyStrategy());
    builder.withCopyStrategy("dynamic");
    Assert.assertEquals("dynamic", builder.build().getCopyStrategy());
  }

  @Test
  public void testTargetPath() {
    final DistCpOptions options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/")).build();
    Assert.assertEquals(new Path("hdfs://localhost:8020/target/"),
        options.getTargetPath());
  }

  @Test
  public void testPreserve() {
    DistCpOptions options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"))
        .build();
    Assert.assertFalse(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.USER));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));

    options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"))
        .preserve(FileAttribute.ACL)
        .build();
    Assert.assertFalse(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.USER));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.ACL));

    options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"))
        .preserve(FileAttribute.BLOCKSIZE)
        .preserve(FileAttribute.REPLICATION)
        .preserve(FileAttribute.PERMISSION)
        .preserve(FileAttribute.USER)
        .preserve(FileAttribute.GROUP)
        .preserve(FileAttribute.CHECKSUMTYPE)
        .build();

    Assert.assertTrue(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.USER));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.XATTR));
  }

  @Test
  public void testAppendOption() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"))
        .withSyncFolder(true)
        .withAppend(true);
    Assert.assertTrue(builder.build().shouldAppend());

    try {
      // make sure -append is only valid when -update is specified
      new DistCpOptions.Builder(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"))
          .withAppend(true)
          .build();
      fail("Append should fail if update option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "Append is valid only with update options", e);
    }

    try {
      // make sure -append is invalid when skipCrc is specified
      new DistCpOptions.Builder(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"))
          .withSyncFolder(true)
          .withAppend(true)
          .withCRC(true)
          .build();
      fail("Append should fail if skipCrc option is specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "Append is disallowed when skipping CRC", e);
    }
  }

  @Test
  public void testDiffOption() {
    DistCpOptions options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"))
        .withSyncFolder(true)
        .withUseDiff("s1", "s2")
        .build();
    Assert.assertTrue(options.shouldUseDiff());
    Assert.assertEquals("s1", options.getFromSnapshot());
    Assert.assertEquals("s2", options.getToSnapshot());

    options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"))
        .withSyncFolder(true)
        .withUseDiff("s1", ".")
        .build();
    Assert.assertTrue(options.shouldUseDiff());
    Assert.assertEquals("s1", options.getFromSnapshot());
    Assert.assertEquals(".", options.getToSnapshot());

    // make sure -diff is only valid when -update is specified
    try {
      new DistCpOptions.Builder(new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"))
          .withUseDiff("s1", "s2")
          .build();
      fail("-diff should fail if -update option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-diff/-rdiff is valid only with -update option", e);
    }

    try {
      new DistCpOptions.Builder(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"))
          .withSyncFolder(true)
          .withUseDiff("s1", "s2")
          .withDeleteMissing(true)
          .build();
      fail("Should fail as -delete and -diff/-rdiff are mutually exclusive.");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-delete and -diff/-rdiff are mutually exclusive.", e);
    }

    try {
      new DistCpOptions.Builder(new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"))
          .withUseDiff("s1", "s2")
          .withDeleteMissing(true)
          .build();
      fail("-diff should fail if -update option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-delete and -diff/-rdiff are mutually exclusive.", e);
    }

    try {
      new DistCpOptions.Builder(new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"))
          .withDeleteMissing(true)
          .withUseDiff("s1", "s2")
          .build();
      fail("Should have failed as -delete and -diff are mutually exclusive");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-delete and -diff/-rdiff are mutually exclusive", e);
    }
  }

  @Test
  public void testExclusionsOption() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(builder.build().getFiltersFile());

    builder.withFiltersFile("/tmp/filters.txt");
    Assert.assertEquals("/tmp/filters.txt", builder.build().getFiltersFile());
  }

  @Test
  public void testSetOptionsForSplitLargeFile() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/"),
        new Path("hdfs://localhost:8020/target/"))
        .withAppend(true)
        .withSyncFolder(true);
    Assert.assertFalse(builder.build().shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertTrue(builder.build().shouldAppend());

    builder.withBlocksPerChunk(5440);
    Assert.assertTrue(builder.build().shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertFalse(builder.build().shouldAppend());
  }

  @Test
  public void testSetCopyBufferSize() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));

    Assert.assertEquals(DistCpConstants.COPY_BUFFER_SIZE_DEFAULT,
        builder.build().getCopyBufferSize());

    builder.withCopyBufferSize(4194304);
    Assert.assertEquals(4194304,
        builder.build().getCopyBufferSize());

    builder.withCopyBufferSize(-1);
    Assert.assertEquals(DistCpConstants.COPY_BUFFER_SIZE_DEFAULT,
        builder.build().getCopyBufferSize());
  }

  @Test
  public void testVerboseLog() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(builder.build().shouldVerboseLog());

    try {
      builder.withVerboseLog(true).build();
      fail("-v should fail if -log option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("-v is valid only with -log option", e);
    }

    final Path logPath = new Path("hdfs://localhost:8020/logs");
    builder.withLogPath(logPath).withVerboseLog(true);
    Assert.assertTrue(builder.build().shouldVerboseLog());
  }

  @Test
  public void testAppendToConf() {
    final int expectedBlocksPerChunk = 999;
    final String expectedValForEmptyConfigKey = "VALUE_OF_EMPTY_CONFIG_KEY";

    DistCpOptions options = new DistCpOptions.Builder(
        Collections.singletonList(
            new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"))
        .withBlocksPerChunk(expectedBlocksPerChunk)
        .build();

    Configuration config = new Configuration();
    config.set("", expectedValForEmptyConfigKey);

    options.appendToConf(config);
    Assert.assertEquals(expectedBlocksPerChunk,
        config.getInt(
            DistCpOptionSwitch
                .BLOCKS_PER_CHUNK
                .getConfigLabel(), 0));
    Assert.assertEquals(
        "Some DistCpOptionSwitch's config label is empty! " +
            "Pls ensure the config label is provided when apply to config, " +
            "otherwise it may not be fetched properly",
            expectedValForEmptyConfigKey, config.get(""));
  }
}
