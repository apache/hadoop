/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.filecache;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test the {@link DistributedCache} class.
 */
public class TestDistributedCache {
  /**
   * Test of addFileOnlyToClassPath method, of class DistributedCache.
   */
  @Test
  public void testAddFileToClassPath() throws Exception {
    Configuration conf = new Configuration(false);

    // Test first with 2 args
    try {
      DistributedCache.addFileToClassPath(null, conf);
      fail("Accepted null archives argument");
    } catch (NullPointerException ex) {
      // Expected
    }

    DistributedCache.addFileToClassPath(new Path("file:///a"), conf);
    assertEquals("The mapreduce.job.classpath.files property was not "
        + "set correctly", "file:/a",
        conf.get(MRJobConfig.CLASSPATH_FILES));
    assertEquals("The mapreduce.job.cache.files property was not set "
        + "correctly", "file:///a", conf.get(MRJobConfig.CACHE_FILES));

    DistributedCache.addFileToClassPath(new Path("file:///b"), conf);
    assertEquals("The mapreduce.job.classpath.files property was not "
        + "set correctly", "file:/a,file:/b",
        conf.get(MRJobConfig.CLASSPATH_FILES));
    assertEquals("The mapreduce.job.cache.files property was not set "
        + "correctly", "file:///a,file:///b",
        conf.get(MRJobConfig.CACHE_FILES));

    // Now test with 3 args
    FileSystem fs = FileSystem.newInstance(conf);
    conf.clear();

    try {
      DistributedCache.addFileToClassPath(null, conf, fs);
      fail("Accepted null archives argument");
    } catch (NullPointerException ex) {
      // Expected
    }

    DistributedCache.addFileToClassPath(
        new Path("file:///a"), conf, fs);
    assertEquals("The mapreduce.job.classpath.files property was not "
        + "set correctly", "file:/a",
        conf.get(MRJobConfig.CLASSPATH_FILES));
    assertEquals("The mapreduce.job.cache.files property was not set "
        + "correctly", "file:///a", conf.get(MRJobConfig.CACHE_FILES));

    DistributedCache.addFileToClassPath(
        new Path("file:///b"), conf, fs);
    assertEquals("The mapreduce.job.classpath.files property was not "
        + "set correctly", "file:/a,file:/b",
        conf.get(MRJobConfig.CLASSPATH_FILES));
    assertEquals("The mapreduce.job.cache.files property was not set "
        + "correctly", "file:///a,file:///b",
        conf.get(MRJobConfig.CACHE_FILES));

    // Now test with 4th arg true
    conf.clear();

    try {
      DistributedCache.addFileToClassPath(null, conf, fs, true);
      fail("Accepted null archives argument");
    } catch (NullPointerException ex) {
      // Expected
    }

    DistributedCache.addFileToClassPath(
        new Path("file:///a"), conf, fs, true);
    assertEquals("The mapreduce.job.classpath.files property was not "
        + "set correctly", "file:/a",
        conf.get(MRJobConfig.CLASSPATH_FILES));
    assertEquals("The mapreduce.job.cache.files property was not set "
        + "correctly", "file:///a", conf.get(MRJobConfig.CACHE_FILES));

    DistributedCache.addFileToClassPath(
        new Path("file:///b"), conf, fs, true);
    assertEquals("The mapreduce.job.classpath.files property was not "
        + "set correctly", "file:/a,file:/b",
        conf.get(MRJobConfig.CLASSPATH_FILES));
    assertEquals("The mapreduce.job.cache.files property was not set "
        + "correctly", "file:///a,file:///b",
        conf.get(MRJobConfig.CACHE_FILES));

    // And finally with 4th arg false
    conf.clear();

    try {
      DistributedCache.addFileToClassPath(null, conf, fs, false);
      fail("Accepted null archives argument");
    } catch (NullPointerException ex) {
      // Expected
    }

    DistributedCache.addFileToClassPath(
        new Path("file:///a"), conf, fs, false);
    assertEquals("The mapreduce.job.classpath.files property was not "
        + "set correctly", "file:/a",
        conf.get(MRJobConfig.CLASSPATH_FILES));
    assertEquals("The mapreduce.job.cache.files property was not set "
        + "correctly", "",
        conf.get(MRJobConfig.CACHE_FILES, ""));

    DistributedCache.addFileToClassPath(
        new Path("file:///b"), conf, fs, false);
    assertEquals("The mapreduce.job.classpath.files property was not "
        + "set correctly", "file:/a,file:/b",
        conf.get(MRJobConfig.CLASSPATH_FILES));
    assertEquals("The mapreduce.job.cache.files property was not set "
        + "correctly", "",
        conf.get(MRJobConfig.CACHE_FILES, ""));
  }

  @Test
  public void testGetFileVisibilities() {
    Configuration conf = new Configuration();
    StringBuilder sb = new StringBuilder();
    for (LocalResourceVisibility vis : LocalResourceVisibility.values()) {
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(vis);
    }
    conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, sb.toString());
    LocalResourceVisibility[] visibilityArr =
        DistributedCache.getFileVisibilities(conf);
    assertEquals("Files resource Visibility Length mismatch",
        LocalResourceVisibility.values().length, visibilityArr.length);
    int i = 0;
    for (LocalResourceVisibility vis : LocalResourceVisibility.values()) {
      assertTrue("Vis mis match",
          vis.name().equals(visibilityArr[i++].name()));
    }
  }

  @Test
  public void testGetArchiveVisibilities(){
    Configuration conf = new Configuration();
    StringBuilder sb = new StringBuilder();
    for (LocalResourceVisibility vis : LocalResourceVisibility.values()) {
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(vis);
    }
    conf.set(MRJobConfig.CACHE_ARCHIVES_VISIBILITIES, sb.toString());
    LocalResourceVisibility[] visibilityArr =
        DistributedCache.getArchiveVisibilities(conf);
    assertEquals("Archives resource Visibility Length mismatch",
        LocalResourceVisibility.values().length, visibilityArr.length);
    int i = 0;
    for (LocalResourceVisibility vis : LocalResourceVisibility.values()) {
      assertTrue("Vis mis match",
          vis.name().equals(visibilityArr[i++].name()));
    }
  }

  @Test
  public void testAddCacheFileWithMeta() throws Exception {
    Configuration conf = new Configuration();
    URI uri = new URI("hdfs://namenode/file1");
    DistributedCache.addCacheFileWithMeta(
        uri, LocalResourceVisibility.PUBLIC, 1000, 0, conf);
    LocalResourceVisibility[] vis = DistributedCache.getFileVisibilities(conf);
    assertEquals("Got more than 1 vis", 1, vis.length);
    assertTrue("Wrong vis",
        vis[0].name().equals(LocalResourceVisibility.PUBLIC.name()));
    long[] timeArr = DistributedCache.getFileTimestamps(conf);
    assertEquals(
        "Got more than 1 timestamp", 1, timeArr.length);
    assertEquals("Wrong timestamp", 1000, timeArr[0]);
    URI[] archivesArr = DistributedCache.getCacheFiles(conf);
    assertEquals(
        "Got more than 1 archive", 1, archivesArr.length);
    assertTrue(
        "Wrong archive", archivesArr[0].toString().equals(uri.toString()));
  }

  @Test
  public void testAddCacheArchiveWithMeta() throws Exception {
    Configuration conf = new Configuration();
    URI uri = new URI("hdfs://namenode/archive1");
    DistributedCache.addCacheArchiveWithMeta(
        uri, LocalResourceVisibility.PUBLIC, 1000, 0, conf);
    LocalResourceVisibility[] vis =
        DistributedCache.getArchiveVisibilities(conf);
    assertEquals("Got more than 1 vis", 1, vis.length);
    assertTrue("Wrong vis",
        vis[0].name().equals(LocalResourceVisibility.PUBLIC.name()));
    long[] timeArr = DistributedCache.getArchiveTimestamps(conf);
    assertEquals(
        "Got more than 1 timestamp", 1, timeArr.length);
    assertEquals("Wrong timestamp", 0, timeArr[0]);
    URI[] archivesArr = DistributedCache.getCacheArchives(conf);
    assertEquals(
        "Got more than 1 archive", 1, archivesArr.length);
    assertTrue(
        "Wrong archive", archivesArr[0].toString().equals(uri.toString()));
  }
}
