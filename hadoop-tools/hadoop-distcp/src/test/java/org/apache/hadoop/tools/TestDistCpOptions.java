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
import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.tools.DistCpOptions.MAX_NUM_LISTSTATUS_THREADS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
    assertFalse(builder.build().shouldIgnoreFailures());

    builder.withIgnoreFailures(true);
    assertTrue(builder.build().shouldIgnoreFailures());
  }

  @Test
  public void testSetOverwrite() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertFalse(builder.build().shouldOverwrite());

    builder.withOverwrite(true);
    assertTrue(builder.build().shouldOverwrite());

    try {
      builder.withSyncFolder(true).build();
      fail("Update and overwrite aren't allowed together");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testLogPath() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertNull(builder.build().getLogPath());

    final Path logPath = new Path("hdfs://localhost:8020/logs");
    builder.withLogPath(logPath);
    assertEquals(logPath, builder.build().getLogPath());
  }

  @Test
  public void testSetBlokcing() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertTrue(builder.build().shouldBlock());

    builder.withBlocking(false);
    assertFalse(builder.build().shouldBlock());
  }

  @Test
  public void testSetBandwidth() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertEquals(0, builder.build().getMapBandwidth(), DELTA);

    builder.withMapBandwidth(11);
    assertEquals(11, builder.build().getMapBandwidth(), DELTA);
  }

  @Test
  public void testSetNonPositiveBandwidth() {
    assertThrows(IllegalArgumentException.class, ()->{
      new DistCpOptions.Builder(
              Collections.singletonList(new Path("hdfs://localhost:8020/source")),
              new Path("hdfs://localhost:8020/target/"))
              .withMapBandwidth(-11)
              .build();
    });
  }

  @Test
  public void testSetZeroBandwidth() {
    assertThrows(IllegalArgumentException.class, () -> {
      new DistCpOptions.Builder(
            Collections.singletonList(new Path("hdfs://localhost:8020/source")),
            new Path("hdfs://localhost:8020/target/"))
            .withMapBandwidth(0)
            .build();
    });
  }

  @Test
  public void testSetSkipCRC() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertFalse(builder.build().shouldSkipCRC());

    final DistCpOptions options = builder.withSyncFolder(true).withSkipCRC(true)
        .build();
    assertTrue(options.shouldSyncFolder());
    assertTrue(options.shouldSkipCRC());
  }

  @Test
  public void testSetAtomicCommit() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertFalse(builder.build().shouldAtomicCommit());

    builder.withAtomicCommit(true);
    assertTrue(builder.build().shouldAtomicCommit());

    try {
      builder.withSyncFolder(true).build();
      fail("Atomic and sync folders were mutually exclusive");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testSetWorkPath() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertNull(builder.build().getAtomicWorkPath());

    builder.withAtomicCommit(true);
    assertNull(builder.build().getAtomicWorkPath());

    final Path workPath = new Path("hdfs://localhost:8020/work");
    builder.withAtomicWorkPath(workPath);
    assertEquals(workPath, builder.build().getAtomicWorkPath());
  }

  @Test
  public void testSetSyncFolders() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertFalse(builder.build().shouldSyncFolder());

    builder.withSyncFolder(true);
    assertTrue(builder.build().shouldSyncFolder());
  }

  @Test
  public void testSetDeleteMissing() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertFalse(builder.build().shouldDeleteMissing());

    DistCpOptions options = builder.withSyncFolder(true)
        .withDeleteMissing(true)
        .build();
    assertTrue(options.shouldSyncFolder());
    assertTrue(options.shouldDeleteMissing());

    options = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"))
        .withOverwrite(true)
        .withDeleteMissing(true)
        .build();
    assertTrue(options.shouldOverwrite());
    assertTrue(options.shouldDeleteMissing());

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
    assertEquals(DistCpConstants.DEFAULT_MAPS,
        builder.build().getMaxMaps());

    builder.maxMaps(1);
    assertEquals(1, builder.build().getMaxMaps());

    builder.maxMaps(0);
    assertEquals(1, builder.build().getMaxMaps());
  }

  @Test
  public void testSetNumListtatusThreads() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    // If command line argument isn't set, we expect .getNumListstatusThreads
    // option to be zero (so that we know when to override conf properties).
    assertEquals(0, builder.build().getNumListstatusThreads());

    builder.withNumListstatusThreads(12);
    assertEquals(12, builder.build().getNumListstatusThreads());

    builder.withNumListstatusThreads(0);
    assertEquals(0, builder.build().getNumListstatusThreads());

    // Ignore large number of threads.
    builder.withNumListstatusThreads(MAX_NUM_LISTSTATUS_THREADS * 2);
    assertEquals(MAX_NUM_LISTSTATUS_THREADS,
        builder.build().getNumListstatusThreads());
  }

  @Test
  public void testSourceListing() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    assertEquals(new Path("hdfs://localhost:8020/source/first"),
        builder.build().getSourceFileListing());
  }

  @Test
  public void testMissingTarget() {
    assertThrows(IllegalArgumentException.class, ()->{
      new DistCpOptions.Builder(new Path("hdfs://localhost:8020/source/first"),
              null);
    });
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
        "directWrite=false, useiterator=false, updateRoot=false}";
    String optionString = option.toString();
    assertEquals(val, optionString);
    assertNotSame(DistCpOptionSwitch.ATOMIC_COMMIT.toString(),
        DistCpOptionSwitch.ATOMIC_COMMIT.name());
  }

  @Test
  public void testCopyStrategy() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    assertEquals(DistCpConstants.UNIFORMSIZE,
        builder.build().getCopyStrategy());
    builder.withCopyStrategy("dynamic");
    assertEquals("dynamic", builder.build().getCopyStrategy());
  }

  @Test
  public void testTargetPath() {
    final DistCpOptions options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/")).build();
    assertEquals(new Path("hdfs://localhost:8020/target/"),
        options.getTargetPath());
  }

  @Test
  public void testPreserve() {
    DistCpOptions options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"))
        .build();
    assertFalse(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    assertFalse(options.shouldPreserve(FileAttribute.REPLICATION));
    assertFalse(options.shouldPreserve(FileAttribute.PERMISSION));
    assertFalse(options.shouldPreserve(FileAttribute.USER));
    assertFalse(options.shouldPreserve(FileAttribute.GROUP));
    assertFalse(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));

    options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"))
        .preserve(FileAttribute.ACL)
        .build();
    assertFalse(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    assertFalse(options.shouldPreserve(FileAttribute.REPLICATION));
    assertFalse(options.shouldPreserve(FileAttribute.PERMISSION));
    assertFalse(options.shouldPreserve(FileAttribute.USER));
    assertFalse(options.shouldPreserve(FileAttribute.GROUP));
    assertFalse(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    assertTrue(options.shouldPreserve(FileAttribute.ACL));

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

    assertTrue(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    assertTrue(options.shouldPreserve(FileAttribute.REPLICATION));
    assertTrue(options.shouldPreserve(FileAttribute.PERMISSION));
    assertTrue(options.shouldPreserve(FileAttribute.USER));
    assertTrue(options.shouldPreserve(FileAttribute.GROUP));
    assertTrue(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    assertFalse(options.shouldPreserve(FileAttribute.XATTR));
  }

  @Test
  public void testAppendOption() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"))
        .withSyncFolder(true)
        .withAppend(true);
    assertTrue(builder.build().shouldAppend());

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
          .withSkipCRC(true)
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
    assertTrue(options.shouldUseDiff());
    assertEquals("s1", options.getFromSnapshot());
    assertEquals("s2", options.getToSnapshot());

    options = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"))
        .withSyncFolder(true)
        .withUseDiff("s1", ".")
        .build();
    assertTrue(options.shouldUseDiff());
    assertEquals("s1", options.getFromSnapshot());
    assertEquals(".", options.getToSnapshot());

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
    assertNull(builder.build().getFiltersFile());

    builder.withFiltersFile("/tmp/filters.txt");
    assertEquals("/tmp/filters.txt", builder.build().getFiltersFile());
  }

  @Test
  public void testSetOptionsForSplitLargeFile() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/"),
        new Path("hdfs://localhost:8020/target/"))
        .withAppend(true)
        .withSyncFolder(true);
    assertFalse(builder.build().shouldPreserve(FileAttribute.BLOCKSIZE));
    assertTrue(builder.build().shouldAppend());

    builder.withBlocksPerChunk(5440);
    assertTrue(builder.build().shouldPreserve(FileAttribute.BLOCKSIZE));
    assertFalse(builder.build().shouldAppend());
  }

  @Test
  public void testSetCopyBufferSize() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));

    assertEquals(DistCpConstants.COPY_BUFFER_SIZE_DEFAULT,
        builder.build().getCopyBufferSize());

    builder.withCopyBufferSize(4194304);
    assertEquals(4194304,
        builder.build().getCopyBufferSize());

    builder.withCopyBufferSize(-1);
    assertEquals(DistCpConstants.COPY_BUFFER_SIZE_DEFAULT,
        builder.build().getCopyBufferSize());
  }

  @Test
  public void testVerboseLog() {
    final DistCpOptions.Builder builder = new DistCpOptions.Builder(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    assertFalse(builder.build().shouldVerboseLog());

    try {
      builder.withVerboseLog(true).build();
      fail("-v should fail if -log option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("-v is valid only with -log option", e);
    }

    final Path logPath = new Path("hdfs://localhost:8020/logs");
    builder.withLogPath(logPath).withVerboseLog(true);
    assertTrue(builder.build().shouldVerboseLog());
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
    assertEquals(expectedBlocksPerChunk,
        config.getInt(
            DistCpOptionSwitch
                .BLOCKS_PER_CHUNK
                .getConfigLabel(), 0));
    assertEquals(expectedValForEmptyConfigKey, config.get(""),
        "Some DistCpOptionSwitch's config label is empty! " +
        "Pls ensure the config label is provided when apply to config, " +
        "otherwise it may not be fetched properly");
  }

  @Test
  public void testUpdateRoot() {
    final DistCpOptions options = new DistCpOptions.Builder(
        Collections.singletonList(
            new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"))
        .withUpdateRoot(true)
        .build();
    assertTrue(options.shouldUpdateRoot());
  }
}
