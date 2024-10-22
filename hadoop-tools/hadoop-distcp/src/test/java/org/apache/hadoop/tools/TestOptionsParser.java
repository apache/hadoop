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

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.assertj.core.api.Assertions.within;
import static org.junit.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.tools.DistCpOptions.*;
import org.apache.hadoop.conf.Configuration;

import java.util.NoSuchElementException;

public class TestOptionsParser {

  private static final float DELTA = 0.001f;

  @Test
  public void testParseIgnoreFailure() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldIgnoreFailures());

    options = OptionsParser.parse(new String[] {
        "-i",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldIgnoreFailures());
  }

  @Test
  public void testParseOverwrite() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldOverwrite());

    options = OptionsParser.parse(new String[] {
        "-overwrite",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldOverwrite());

    try {
      OptionsParser.parse(new String[] {
          "-update",
          "-overwrite",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/"});
      Assert.fail("Update and overwrite aren't allowed together");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testLogPath() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertNull(options.getLogPath());

    options = OptionsParser.parse(new String[] {
        "-log",
        "hdfs://localhost:8020/logs",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(options.getLogPath(), new Path("hdfs://localhost:8020/logs"));
  }

  @Test
  public void testParseBlokcing() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldBlock());

    options = OptionsParser.parse(new String[] {
        "-async",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldBlock());
  }

  @Test
  public void testParsebandwidth() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    assertThat(options.getMapBandwidth()).isCloseTo(0f, within(DELTA));

    options = OptionsParser.parse(new String[] {
        "-bandwidth",
        "11.2",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    assertThat(options.getMapBandwidth()).isCloseTo(11.2f, within(DELTA));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testParseNonPositiveBandwidth() {
    OptionsParser.parse(new String[] {
        "-bandwidth",
        "-11",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
  }

  @Test(expected=IllegalArgumentException.class)
  public void testParseZeroBandwidth() {
    OptionsParser.parse(new String[] {
        "-bandwidth",
        "0",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
  }

  @Test
  public void testParseSkipCRC() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldSkipCRC());

    options = OptionsParser.parse(new String[] {
        "-update",
        "-skipcrccheck",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldSyncFolder());
    Assert.assertTrue(options.shouldSkipCRC());
  }

  @Test
  public void testParseAtomicCommit() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldAtomicCommit());

    options = OptionsParser.parse(new String[] {
        "-atomic",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldAtomicCommit());

    try {
      OptionsParser.parse(new String[] {
          "-atomic",
          "-update",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/"});
      Assert.fail("Atomic and sync folders were allowed");
    } catch (IllegalArgumentException ignore) { }
  }

  @Test
  public void testParseWorkPath() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertNull(options.getAtomicWorkPath());

    options = OptionsParser.parse(new String[] {
        "-atomic",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertNull(options.getAtomicWorkPath());

    options = OptionsParser.parse(new String[] {
        "-atomic",
        "-tmp",
        "hdfs://localhost:8020/work",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(options.getAtomicWorkPath(), new Path("hdfs://localhost:8020/work"));

    try {
      OptionsParser.parse(new String[] {
          "-tmp",
          "hdfs://localhost:8020/work",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/"});
      Assert.fail("work path was allowed without -atomic switch");
    } catch (IllegalArgumentException ignore) {}
  }

  @Test
  public void testParseSyncFolders() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldSyncFolder());

    options = OptionsParser.parse(new String[] {
        "-update",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldSyncFolder());
  }

  @Test
  public void testParseDeleteMissing() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldDeleteMissing());

    options = OptionsParser.parse(new String[] {
        "-update",
        "-delete",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldSyncFolder());
    Assert.assertTrue(options.shouldDeleteMissing());

    options = OptionsParser.parse(new String[] {
        "-overwrite",
        "-delete",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldOverwrite());
    Assert.assertTrue(options.shouldDeleteMissing());

    try {
      OptionsParser.parse(new String[] {
          "-atomic",
          "-delete",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/"});
      Assert.fail("Atomic and delete folders were allowed");
    } catch (IllegalArgumentException ignore) { }
  }

  @Test
  public void testParseMaps() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    assertThat(options.getMaxMaps()).isEqualTo(DistCpConstants.DEFAULT_MAPS);

    options = OptionsParser.parse(new String[] {
        "-m",
        "1",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    assertThat(options.getMaxMaps()).isEqualTo(1);

    options = OptionsParser.parse(new String[] {
        "-m",
        "0",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    assertThat(options.getMaxMaps()).isEqualTo(1);

    try {
      OptionsParser.parse(new String[] {
          "-m",
          "hello",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/"});
      Assert.fail("Non numberic map parsed");
    } catch (IllegalArgumentException ignore) { }

    try {
      OptionsParser.parse(new String[] {
          "-mapredXslConf",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/"});
      Assert.fail("Non numberic map parsed");
    } catch (IllegalArgumentException ignore) { }
  }

  @Test
  public void testParseNumListstatusThreads() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    // If command line argument isn't set, we expect .getNumListstatusThreads
    // option to be zero (so that we know when to override conf properties).
    Assert.assertEquals(0, options.getNumListstatusThreads());

    options = OptionsParser.parse(new String[] {
        "--numListstatusThreads",
        "12",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(12, options.getNumListstatusThreads());

    options = OptionsParser.parse(new String[] {
        "--numListstatusThreads",
        "0",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(0, options.getNumListstatusThreads());

    try {
      OptionsParser.parse(new String[] {
          "--numListstatusThreads",
          "hello",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/"});
      Assert.fail("Non numberic numListstatusThreads parsed");
    } catch (IllegalArgumentException ignore) { }

    // Ignore large number of threads.
    options = OptionsParser.parse(new String[] {
        "--numListstatusThreads",
        "100",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(DistCpOptions.MAX_NUM_LISTSTATUS_THREADS,
                        options.getNumListstatusThreads());
  }

  @Test
  public void testSourceListing() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(options.getSourceFileListing(),
        new Path("hdfs://localhost:8020/source/first"));
  }

  @Test
  public void testSourceListingAndSourcePath() {
    try {
      OptionsParser.parse(new String[] {
          "-f",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/"});
      Assert.fail("Both source listing & source paths allowed");
    } catch (IllegalArgumentException ignore) {}
  }

  @Test
  public void testMissingSourceInfo() {
    try {
      OptionsParser.parse(new String[] {
          "hdfs://localhost:8020/target/"});
      Assert.fail("Neither source listing not source paths present");
    } catch (IllegalArgumentException ignore) {}
  }

  @Test
  public void testMissingTarget() {
    try {
      OptionsParser.parse(new String[] {
          "-f", "hdfs://localhost:8020/source"});
      Assert.fail("Missing target allowed");
    } catch (IllegalArgumentException ignore) {}
  }

  @Test
  public void testInvalidArgs() {
    try {
      OptionsParser.parse(new String[] {
          "-m", "-f", "hdfs://localhost:8020/source"});
      Assert.fail("Missing map value");
    } catch (IllegalArgumentException ignore) {}
  }

  @Test
  public void testCopyStrategy() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "-strategy",
        "dynamic",
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    assertThat(options.getCopyStrategy()).isEqualTo("dynamic");

    options = OptionsParser.parse(new String[] {
         "-strategy",
         "record",
         "-f",
         "hdfs://localhost:8020/source/first",
         "hdfs://localhost:8020/target/"});
    assertThat(options.getCopyStrategy()).isEqualTo("record");

    options = OptionsParser.parse(new String[] {
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    assertThat(options.getCopyStrategy()).isEqualTo(DistCpConstants.UNIFORMSIZE);
  }

  @Test
  public void testTargetPath() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(options.getTargetPath(), new Path("hdfs://localhost:8020/target/"));
  }

  @Test
  public void testPreserve() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.USER));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));

    options = OptionsParser.parse(new String[] {
        "-p",
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.USER));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.ACL));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.XATTR));

    options = OptionsParser.parse(new String[] {
        "-p",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.USER));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.ACL));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.XATTR));

    options = OptionsParser.parse(new String[] {
        "-pbr",
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.USER));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.ACL));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.XATTR));

    options = OptionsParser.parse(new String[] {
        "-pbrgup",
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.USER));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.ACL));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.XATTR));

    options = OptionsParser.parse(new String[] {
        "-pbrgupcaxt",
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.USER));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.ACL));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.XATTR));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.TIMES));

    options = OptionsParser.parse(new String[] {
        "-pc",
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldPreserve(FileAttribute.BLOCKSIZE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.REPLICATION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.PERMISSION));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.USER));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.GROUP));
    Assert.assertTrue(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.ACL));
    Assert.assertFalse(options.shouldPreserve(FileAttribute.XATTR));

    options = OptionsParser.parse(new String[] {
        "-p",
        "-f",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertEquals(DistCpOptionSwitch.PRESERVE_STATUS_DEFAULT.length() - 2,
        options.getPreserveAttributes().size());

    try {
      OptionsParser.parse(new String[] {
          "-pabcd",
          "-f",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target"});
      Assert.fail("Invalid preserve attribute");
    }
    catch (NoSuchElementException ignore) {}

    Builder builder = new DistCpOptions.Builder(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(
        builder.build().shouldPreserve(FileAttribute.PERMISSION));
    builder.preserve(FileAttribute.PERMISSION);
    Assert.assertTrue(builder.build().shouldPreserve(FileAttribute.PERMISSION));

    builder.preserve(FileAttribute.PERMISSION);
    Assert.assertTrue(builder.build().shouldPreserve(FileAttribute.PERMISSION));
  }

  @Test
  public void testOptionsSwitchAddToConf() {
    Configuration conf = new Configuration();
    Assert.assertNull(conf.get(DistCpOptionSwitch.ATOMIC_COMMIT.getConfigLabel()));
    DistCpOptionSwitch.addToConf(conf, DistCpOptionSwitch.ATOMIC_COMMIT);
    Assert.assertTrue(conf.getBoolean(DistCpOptionSwitch.ATOMIC_COMMIT.getConfigLabel(), false));
  }

  @Test
  public void testOptionsAppendToConf() {
    Configuration conf = new Configuration();
    Assert.assertFalse(conf.getBoolean(DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(), false));
    Assert.assertFalse(conf.getBoolean(DistCpOptionSwitch.ATOMIC_COMMIT.getConfigLabel(), false));
    Assert.assertEquals(
        conf.getRaw(DistCpOptionSwitch.BANDWIDTH.getConfigLabel()), null);
    DistCpOptions options = OptionsParser.parse(new String[] {
        "-atomic",
        "-i",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    options.appendToConf(conf);
    Assert.assertTrue(conf.getBoolean(DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(), false));
    Assert.assertTrue(conf.getBoolean(DistCpOptionSwitch.ATOMIC_COMMIT.getConfigLabel(), false));
    Assert.assertEquals(conf.getFloat(DistCpOptionSwitch.BANDWIDTH.getConfigLabel(), -1),
        -1.0, DELTA);

    conf = new Configuration();
    Assert.assertFalse(conf.getBoolean(DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(), false));
    Assert.assertFalse(conf.getBoolean(DistCpOptionSwitch.DELETE_MISSING.getConfigLabel(), false));
    assertThat(conf.get(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel())).isNull();
    options = OptionsParser.parse(new String[] {
        "-update",
        "-delete",
        "-pu",
        "-bandwidth",
        "11.2",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    options.appendToConf(conf);
    Assert.assertTrue(conf.getBoolean(DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(), false));
    Assert.assertTrue(conf.getBoolean(DistCpOptionSwitch.DELETE_MISSING.getConfigLabel(), false));
    assertThat(conf.get(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel())).isEqualTo("U");
    assertThat(conf.getFloat(DistCpOptionSwitch.BANDWIDTH.getConfigLabel(), -1))
        .isCloseTo(11.2f, within(DELTA));
  }

  @Test
  public void testOptionsAppendToConfDoesntOverwriteBandwidth() {
    Configuration conf = new Configuration();
    Assert.assertEquals(
        conf.getRaw(DistCpOptionSwitch.BANDWIDTH.getConfigLabel()), null);
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    options.appendToConf(conf);
    assertThat(conf.getFloat(DistCpOptionSwitch.BANDWIDTH.getConfigLabel(), -1))
            .isCloseTo(-1.0f,within(DELTA));

    conf = new Configuration();
    Assert.assertEquals(
        conf.getRaw(DistCpOptionSwitch.BANDWIDTH.getConfigLabel()), null);
    options = OptionsParser.parse(new String[] {
        "-update",
        "-delete",
        "-pu",
        "-bandwidth",
        "77",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    options.appendToConf(conf);
    Assert.assertEquals(
        conf.getFloat(DistCpOptionSwitch.BANDWIDTH.getConfigLabel(), -1), 77.0,
        DELTA);

    conf = new Configuration();
    conf.set(DistCpOptionSwitch.BANDWIDTH.getConfigLabel(), "88");
    Assert.assertEquals(
        conf.getRaw(DistCpOptionSwitch.BANDWIDTH.getConfigLabel()), "88");
    options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    options.appendToConf(conf);
    Assert.assertEquals(
        conf.getFloat(DistCpOptionSwitch.BANDWIDTH.getConfigLabel(), -1), 88.0,
        DELTA);

    conf = new Configuration();
    conf.set(DistCpOptionSwitch.BANDWIDTH.getConfigLabel(), "88.0");
    Assert.assertEquals(
        conf.getRaw(DistCpOptionSwitch.BANDWIDTH.getConfigLabel()), "88.0");
    options = OptionsParser.parse(new String[] {
        "-bandwidth",
        "99",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    options.appendToConf(conf);
    Assert.assertEquals(
        conf.getFloat(DistCpOptionSwitch.BANDWIDTH.getConfigLabel(), -1), 99.0,
        DELTA);
  }

  @Test
  public void testAppendOption() {
    Configuration conf = new Configuration();
    Assert.assertFalse(conf.getBoolean(
        DistCpOptionSwitch.APPEND.getConfigLabel(), false));
    Assert.assertFalse(conf.getBoolean(
        DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(), false));

    DistCpOptions options = OptionsParser.parse(new String[] { "-update",
        "-append", "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/" });
    options.appendToConf(conf);
    Assert.assertTrue(conf.getBoolean(
        DistCpOptionSwitch.APPEND.getConfigLabel(), false));
    Assert.assertTrue(conf.getBoolean(
        DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(), false));

    // make sure -append is only valid when -update is specified
    try {
      OptionsParser.parse(new String[] { "-append",
              "hdfs://localhost:8020/source/first",
              "hdfs://localhost:8020/target/" });
      fail("Append should fail if update option is not specified");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "Append is valid only with update options", e);
    }

    // make sure -append is invalid when skipCrc is specified
    try {
      OptionsParser.parse(new String[] {
          "-append", "-update", "-skipcrccheck",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/" });
      fail("Append should fail if skipCrc option is specified");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "Append is disallowed when skipping CRC", e);
    }
  }

  // Test -diff or -rdiff
  private void testSnapshotDiffOption(boolean isDiff) {
    final String optionStr = isDiff? "-diff" : "-rdiff";
    final String optionLabel = isDiff?
        DistCpOptionSwitch.DIFF.getConfigLabel() :
          DistCpOptionSwitch.RDIFF.getConfigLabel();
    Configuration conf = new Configuration();
    Assert.assertFalse(conf.getBoolean(optionLabel, false));

    DistCpOptions options = OptionsParser.parse(new String[] { "-update",
        optionStr, "s1", "s2",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/" });
    options.appendToConf(conf);
    Assert.assertTrue(conf.getBoolean(optionLabel, false));
    Assert.assertTrue(isDiff?
        options.shouldUseDiff() : options.shouldUseRdiff());
    Assert.assertEquals("s1", options.getFromSnapshot());
    Assert.assertEquals("s2", options.getToSnapshot());

    options = OptionsParser.parse(new String[] {
        optionStr, "s1", ".", "-update",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/" });
    options.appendToConf(conf);
    Assert.assertTrue(conf.getBoolean(optionLabel, false));
    Assert.assertTrue(isDiff?
        options.shouldUseDiff() : options.shouldUseRdiff());
    Assert.assertEquals("s1", options.getFromSnapshot());
    Assert.assertEquals(".", options.getToSnapshot());

    // -diff/-rdiff requires two option values
    try {
      OptionsParser.parse(new String[] {optionStr, "s1", "-update",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/" });
      fail(optionStr + " should fail with only one snapshot name");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "Must provide both the starting and ending snapshot names", e);
    }

    // make sure -diff/-rdiff is only valid when -update is specified
    try {
      OptionsParser.parse(new String[] {optionStr, "s1", "s2",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/" });
      fail(optionStr + " should fail if -update option is not specified");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "-diff/-rdiff is valid only with -update option", e);
    }

    try {
      OptionsParser.parse(new String[] {
          "-diff", "s1", "s2", "-update", "-delete",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/" });
      fail("Should fail as -delete and -diff/-rdiff are mutually exclusive");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-delete and -diff/-rdiff are mutually exclusive", e);
    }

    try {
      OptionsParser.parse(new String[] {
          "-diff", "s1", "s2", "-delete",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/" });
      fail("Should fail as -delete and -diff/-rdiff are mutually exclusive");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-delete and -diff/-rdiff are mutually exclusive", e);
    }

    try {
      OptionsParser.parse(new String[] {optionStr, "s1", "s2",
          "-delete", "-overwrite",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/" });
      fail("Should fail as -delete and -diff are mutually exclusive");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-delete and -diff/-rdiff are mutually exclusive", e);
    }

    final String optionStrOther = isDiff? "-rdiff" : "-diff";
    try {
      OptionsParser.parse(new String[] {
          optionStr, "s1", "s2",
          optionStrOther, "s2", "s1",
          "-update",
          "hdfs://localhost:8020/source/first",
          "hdfs://localhost:8020/target/" });
      fail(optionStr + " should fail if " + optionStrOther
          + " is also specified");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains(
          "-diff and -rdiff are mutually exclusive", e);
    }
  }

  @Test
  public void testDiffOption() {
    testSnapshotDiffOption(true);
  }

  @Test
  public void testRdiffOption() {
    testSnapshotDiffOption(false);
  }

  @Test
  public void testExclusionsOption() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertNull(options.getFiltersFile());

    options = OptionsParser.parse(new String[] {
        "-filters",
        "/tmp/filters.txt",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    assertThat(options.getFiltersFile()).isEqualTo("/tmp/filters.txt");
  }

  @Test
  public void testParseUpdateRoot() {
    DistCpOptions options = OptionsParser.parse(new String[] {
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertFalse(options.shouldUpdateRoot());

    options = OptionsParser.parse(new String[] {
        "-updateRoot",
        "hdfs://localhost:8020/source/first",
        "hdfs://localhost:8020/target/"});
    Assert.assertTrue(options.shouldUpdateRoot());
  }
}
