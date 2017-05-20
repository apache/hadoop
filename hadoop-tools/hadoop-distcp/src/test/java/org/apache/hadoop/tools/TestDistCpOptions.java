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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.apache.hadoop.tools.DistCpOptions.maxNumListstatusThreads;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * This is to test constructing {@link DistCpOptions} manually with setters.
 *
 * The test cases in this class is very similar to the parser test, see
 * {@link TestOptionsParser}.
 */
public class TestDistCpOptions {

  @Test
  public void testSetIgnoreFailure() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(options.shouldIgnoreFailures());

    options.setIgnoreFailures(true);
    Assert.assertTrue(options.shouldIgnoreFailures());
  }

  @Test
  public void testSetOverwrite() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(options.shouldOverwrite());

    options.setOverwrite(true);
    Assert.assertTrue(options.shouldOverwrite());

    try {
      options.setSyncFolder(true);
      Assert.fail("Update and overwrite aren't allowed together");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testLogPath() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(options.getLogPath());

    final Path logPath = new Path("hdfs://localhost:8020/logs");
    options.setLogPath(logPath);
    Assert.assertEquals(logPath, options.getLogPath());
  }

  @Test
  public void testSetBlokcing() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertTrue(options.shouldBlock());

    options.setBlocking(false);
    Assert.assertFalse(options.shouldBlock());
  }

  @Test
  public void testSetBandwidth() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertEquals(DistCpConstants.DEFAULT_BANDWIDTH_MB,
        options.getMapBandwidth());

    options.setMapBandwidth(11);
    Assert.assertEquals(11, options.getMapBandwidth());
  }

  @Test(expected = AssertionError.class)
  public void testSetNonPositiveBandwidth() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    options.setMapBandwidth(-11);
  }

  @Test(expected = AssertionError.class)
  public void testSetZeroBandwidth() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    options.setMapBandwidth(0);
  }

  @Test
  public void testSetSkipCRC() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(options.shouldSkipCRC());

    options.setSyncFolder(true);
    options.setSkipCRC(true);
    Assert.assertTrue(options.shouldSyncFolder());
    Assert.assertTrue(options.shouldSkipCRC());
  }

  @Test
  public void testSetAtomicCommit() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(options.shouldAtomicCommit());

    options.setAtomicCommit(true);
    Assert.assertTrue(options.shouldAtomicCommit());

    try {
      options.setSyncFolder(true);
      Assert.fail("Atomic and sync folders were mutually exclusive");
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testSetWorkPath() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(options.getAtomicWorkPath());

    options.setAtomicCommit(true);
    Assert.assertNull(options.getAtomicWorkPath());

    final Path workPath = new Path("hdfs://localhost:8020/work");
    options.setAtomicWorkPath(workPath);
    Assert.assertEquals(workPath,
        options.getAtomicWorkPath());
  }

  @Test
  public void testSetSyncFolders() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertFalse(options.shouldSyncFolder());

    options.setSyncFolder(true);
    Assert.assertTrue(options.shouldSyncFolder());
  }

  @Test
  public void testSetDeleteMissing() {
    {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"));
      Assert.assertFalse(options.shouldDeleteMissing());

      options.setSyncFolder(true);
      options.setDeleteMissing(true);
      Assert.assertTrue(options.shouldSyncFolder());
      Assert.assertTrue(options.shouldDeleteMissing());
    }
    {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"));
      options.setOverwrite(true);
      options.setDeleteMissing(true);
      Assert.assertTrue(options.shouldOverwrite());
      Assert.assertTrue(options.shouldDeleteMissing());
    }
    try {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"));
      options.setDeleteMissing(true);
      fail("Delete missing should fail without update or overwrite options");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("Delete missing is applicable only with update " +
          "or overwrite options", e);
    }
    try {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      options.setSyncFolder(true);
      options.setDeleteMissing(true);
      options.setUseDiff("s1", "s2");
      assertFalse("-delete should be ignored when -diff is specified",
          options.shouldDeleteMissing());
    } catch (IllegalArgumentException e) {
      fail("Got unexpected IllegalArgumentException: " + e.getMessage());
    }
  }

  @Test
  public void testSetSSLConf() {
    final DistCpOptions options = new DistCpOptions(
        Collections.singletonList(new Path("hdfs://localhost:8020/source")),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(options.getSslConfigurationFile());

    options.setSslConfigurationFile("/tmp/ssl-client.xml");
    Assert.assertEquals("/tmp/ssl-client.xml",
        options.getSslConfigurationFile());
  }

  @Test
  public void testSetMaps() {
    {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"));
      Assert.assertEquals(DistCpConstants.DEFAULT_MAPS, options.getMaxMaps());

      options.setMaxMaps(1);
      Assert.assertEquals(1, options.getMaxMaps());
    }
    {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"));
      options.setMaxMaps(0);
      Assert.assertEquals(1, options.getMaxMaps());
    }
  }

  @Test
  public void testSetNumListtatusThreads() {
    {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"));
      // If command line argument isn't set, we expect .getNumListstatusThreads
      // option to be zero (so that we know when to override conf properties).
      Assert.assertEquals(0, options.getNumListstatusThreads());

      options.setNumListstatusThreads(12);
      Assert.assertEquals(12, options.getNumListstatusThreads());
    }

    {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"));
      options.setNumListstatusThreads(0);
      Assert.assertEquals(0, options.getNumListstatusThreads());
    }

    // Ignore large number of threads.
    {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source")),
          new Path("hdfs://localhost:8020/target/"));
      options.setNumListstatusThreads(maxNumListstatusThreads * 2);
      Assert.assertEquals(maxNumListstatusThreads,
          options.getNumListstatusThreads());
    }
  }

  @Test
  public void testSourceListing() {
    final DistCpOptions options = new DistCpOptions(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertEquals(new Path("hdfs://localhost:8020/source/first"),
        options.getSourceFileListing());
  }

  @Test(expected = AssertionError.class)
  public void testMissingTarget() {
    new DistCpOptions(new Path("hdfs://localhost:8020/source/first"), null);
  }

  @Test
  public void testToString() {
    DistCpOptions option = new DistCpOptions(new Path("abc"), new Path("xyz"));
    final String val = "DistCpOptions{atomicCommit=false, syncFolder=false, "
        + "deleteMissing=false, ignoreFailures=false, overwrite=false, "
        + "append=false, useDiff=false, useRdiff=false, "
        + "fromSnapshot=null, toSnapshot=null, "
        + "skipCRC=false, blocking=true, numListstatusThreads=0, maxMaps=20, "
        + "mapBandwidth=100, sslConfigurationFile='null', "
        + "copyStrategy='uniformsize', preserveStatus=[], "
        + "preserveRawXattrs=false, atomicWorkPath=null, logPath=null, "
        + "sourceFileListing=abc, sourcePaths=null, targetPath=xyz, "
        + "targetPathExists=true, filtersFile='null', blocksPerChunk=0, "
        + "copyBufferSize=8192}";
    String optionString = option.toString();
    Assert.assertEquals(val, optionString);
    Assert.assertNotSame(DistCpOptionSwitch.ATOMIC_COMMIT.toString(),
        DistCpOptionSwitch.ATOMIC_COMMIT.name());
  }

  @Test
  public void testCopyStrategy() {
    {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      Assert.assertEquals(DistCpConstants.UNIFORMSIZE,
          options.getCopyStrategy());
      options.setCopyStrategy("dynamic");
      Assert.assertEquals("dynamic", options.getCopyStrategy());
    }
  }

  @Test
  public void testTargetPath() {
    final DistCpOptions options = new DistCpOptions(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertEquals(new Path("hdfs://localhost:8020/target/"),
        options.getTargetPath());
  }

  @Test
  public void testPreserve() {
    {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.BLOCKSIZE));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.REPLICATION));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.PERMISSION));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.USER));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.GROUP));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
    }
    {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      options.preserve(FileAttribute.ACL);
      Assert.assertFalse(options.shouldPreserve(FileAttribute.BLOCKSIZE));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.REPLICATION));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.PERMISSION));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.USER));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.GROUP));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
      Assert.assertTrue(options.shouldPreserve(FileAttribute.ACL));
    }
    {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source/")),
          new Path("hdfs://localhost:8020/target/"));
      options.preserve(FileAttribute.BLOCKSIZE);
      options.preserve(FileAttribute.REPLICATION);
      options.preserve(FileAttribute.PERMISSION);
      options.preserve(FileAttribute.USER);
      options.preserve(FileAttribute.GROUP);
      options.preserve(FileAttribute.CHECKSUMTYPE);

      Assert.assertTrue(options.shouldPreserve(FileAttribute.BLOCKSIZE));
      Assert.assertTrue(options.shouldPreserve(FileAttribute.REPLICATION));
      Assert.assertTrue(options.shouldPreserve(FileAttribute.PERMISSION));
      Assert.assertTrue(options.shouldPreserve(FileAttribute.USER));
      Assert.assertTrue(options.shouldPreserve(FileAttribute.GROUP));
      Assert.assertTrue(options.shouldPreserve(FileAttribute.CHECKSUMTYPE));
      Assert.assertFalse(options.shouldPreserve(FileAttribute.XATTR));
    }
  }

  @Test
  public void testAppendOption() {
    {
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source/")),
          new Path("hdfs://localhost:8020/target/"));
      options.setSyncFolder(true);
      options.setAppend(true);
      Assert.assertTrue(options.shouldAppend());
    }

    try {
      // make sure -append is only valid when -update is specified
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source/")),
          new Path("hdfs://localhost:8020/target/"));
      options.setAppend(true);
      fail("Append should fail if update option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "Append is valid only with update options", e);
    }

    try {
      // make sure -append is invalid when skipCrc is specified
      final DistCpOptions options = new DistCpOptions(
          Collections.singletonList(new Path("hdfs://localhost:8020/source/")),
          new Path("hdfs://localhost:8020/target/"));
      options.setSyncFolder(true);
      options.setAppend(true);
      options.setSkipCRC(true);
      fail("Append should fail if skipCrc option is specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "Append is disallowed when skipping CRC", e);
    }
  }

  @Test
  public void testDiffOption() {
    {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      options.setSyncFolder(true);
      options.setUseDiff("s1", "s2");
      Assert.assertTrue(options.shouldUseDiff());
      Assert.assertEquals("s1", options.getFromSnapshot());
      Assert.assertEquals("s2", options.getToSnapshot());
    }
    {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      options.setSyncFolder(true);
      options.setUseDiff("s1", ".");
      Assert.assertTrue(options.shouldUseDiff());
      Assert.assertEquals("s1", options.getFromSnapshot());
      Assert.assertEquals(".", options.getToSnapshot());
    }

    // make sure -diff is only valid when -update is specified
    try {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      options.setUseDiff("s1", "s2");
      fail("-diff should fail if -update option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-diff/-rdiff is valid only with -update option", e);
    }

    try {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      options.setSyncFolder(true);
      options.setUseDiff("s1", "s2");
      options.setDeleteMissing(true);
      assertFalse("-delete should be ignored when -diff is specified",
          options.shouldDeleteMissing());
    } catch (IllegalArgumentException e) {
      fail("Got unexpected IllegalArgumentException: " + e.getMessage());
    }

    try {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      options.setUseDiff("s1", "s2");
      options.setDeleteMissing(true);
      fail("-diff should fail if -update option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains(
          "-diff/-rdiff is valid only with -update option", e);
    }

    try {
      final DistCpOptions options = new DistCpOptions(
          new Path("hdfs://localhost:8020/source/first"),
          new Path("hdfs://localhost:8020/target/"));
      options.setDeleteMissing(true);
      options.setUseDiff("s1", "s2");
      fail("-delete should fail if -update option is not specified");
    } catch (IllegalArgumentException e) {
      assertExceptionContains("Delete missing is applicable only with update " +
          "or overwrite options", e);
    }
  }

  @Test
  public void testExclusionsOption() {
    DistCpOptions options = new DistCpOptions(
        new Path("hdfs://localhost:8020/source/first"),
        new Path("hdfs://localhost:8020/target/"));
    Assert.assertNull(options.getFiltersFile());

    options.setFiltersFile("/tmp/filters.txt");
    Assert.assertEquals("/tmp/filters.txt", options.getFiltersFile());
  }
}
