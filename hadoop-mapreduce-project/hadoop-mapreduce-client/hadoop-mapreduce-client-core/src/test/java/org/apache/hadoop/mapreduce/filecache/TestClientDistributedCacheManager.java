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
package org.apache.hadoop.mapreduce.filecache;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

public class TestClientDistributedCacheManager {
  private static final Log LOG = LogFactory.getLog(
      TestClientDistributedCacheManager.class);
  
  private static final Path TEST_ROOT_DIR = new Path(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
      TestClientDistributedCacheManager.class.getSimpleName());

  private static final Path TEST_VISIBILITY_PARENT_DIR =
      new Path(TEST_ROOT_DIR, "TestCacheVisibility_Parent");

  private static final Path TEST_VISIBILITY_CHILD_DIR =
      new Path(TEST_VISIBILITY_PARENT_DIR, "TestCacheVisibility_Child");

  private static final String FIRST_CACHE_FILE = "firstcachefile";
  private static final String SECOND_CACHE_FILE = "secondcachefile";

  private FileSystem fs;
  private Path firstCacheFile;
  private Path secondCacheFile;
  private Configuration conf;
  
  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    fs = FileSystem.get(conf);
    firstCacheFile = new Path(TEST_VISIBILITY_PARENT_DIR, FIRST_CACHE_FILE);
    secondCacheFile = new Path(TEST_VISIBILITY_CHILD_DIR, SECOND_CACHE_FILE);
    createTempFile(firstCacheFile, conf);
    createTempFile(secondCacheFile, conf);
  }
  
  @After
  public void tearDown() throws IOException {
    if (fs.delete(TEST_ROOT_DIR, true)) {
      LOG.warn("Failed to delete test root dir and its content under "
          + TEST_ROOT_DIR);
    }
  }
  
  @Test
  public void testDetermineTimestamps() throws IOException {
    Job job = Job.getInstance(conf);
    job.addCacheFile(firstCacheFile.toUri());
    job.addCacheFile(secondCacheFile.toUri());
    Configuration jobConf = job.getConfiguration();
    
    Map<URI, FileStatus> statCache = new HashMap<>();
    ClientDistributedCacheManager.determineTimestamps(jobConf, statCache);
    
    FileStatus firstStatus = statCache.get(firstCacheFile.toUri());
    FileStatus secondStatus = statCache.get(secondCacheFile.toUri());
    
    Assert.assertNotNull(firstCacheFile + " was not found in the stats cache",
        firstStatus);
    Assert.assertNotNull(secondCacheFile + " was not found in the stats cache",
        secondStatus);
    Assert.assertEquals("Missing/extra entries found in the stas cache",
        2, statCache.size());
    String expected = firstStatus.getModificationTime() + ","
        + secondStatus.getModificationTime();
    Assert.assertEquals(expected, jobConf.get(MRJobConfig.CACHE_FILE_TIMESTAMPS));

    job = Job.getInstance(conf);
    job.addCacheFile(new Path(TEST_VISIBILITY_CHILD_DIR, "*").toUri());
    jobConf = job.getConfiguration();
    statCache.clear();
    ClientDistributedCacheManager.determineTimestamps(jobConf, statCache);

    FileStatus thirdStatus = statCache.get(TEST_VISIBILITY_CHILD_DIR.toUri());

    Assert.assertEquals("Missing/extra entries found in the stas cache",
        1, statCache.size());
    Assert.assertNotNull(TEST_VISIBILITY_CHILD_DIR
        + " was not found in the stats cache", thirdStatus);
    expected = Long.toString(thirdStatus.getModificationTime());
    Assert.assertEquals("Incorrect timestamp for " + TEST_VISIBILITY_CHILD_DIR,
        expected, jobConf.get(MRJobConfig.CACHE_FILE_TIMESTAMPS));
  }
  
  @Test
  public void testDetermineCacheVisibilities() throws IOException {
    fs.setPermission(TEST_VISIBILITY_PARENT_DIR,
        new FsPermission((short)00777));
    fs.setPermission(TEST_VISIBILITY_CHILD_DIR,
        new FsPermission((short)00777));
    fs.setWorkingDirectory(TEST_VISIBILITY_CHILD_DIR);
    Job job = Job.getInstance(conf);
    Path relativePath = new Path(SECOND_CACHE_FILE);
    Path wildcardPath = new Path("*");
    Map<URI, FileStatus> statCache = new HashMap<>();
    Configuration jobConf;

    job.addCacheFile(firstCacheFile.toUri());
    job.addCacheFile(relativePath.toUri());
    jobConf = job.getConfiguration();

    ClientDistributedCacheManager.determineCacheVisibilities(jobConf,
        statCache);
    // We use get() instead of getBoolean() so we can tell the difference
    // between wrong and missing
    assertEquals("The file paths were not found to be publicly visible "
        + "even though the full path is publicly accessible",
        "true,true", jobConf.get(MRJobConfig.CACHE_FILE_VISIBILITIES));
    checkCacheEntries(statCache, null, firstCacheFile, relativePath);

    job = Job.getInstance(conf);
    job.addCacheFile(wildcardPath.toUri());
    jobConf = job.getConfiguration();
    statCache.clear();

    ClientDistributedCacheManager.determineCacheVisibilities(jobConf,
        statCache);
    // We use get() instead of getBoolean() so we can tell the difference
    // between wrong and missing
    assertEquals("The file path was not found to be publicly visible "
        + "even though the full path is publicly accessible",
        "true", jobConf.get(MRJobConfig.CACHE_FILE_VISIBILITIES));
    checkCacheEntries(statCache, null, wildcardPath.getParent());

    Path qualifiedParent = fs.makeQualified(TEST_VISIBILITY_PARENT_DIR);
    fs.setPermission(TEST_VISIBILITY_PARENT_DIR,
        new FsPermission((short)00700));
    job = Job.getInstance(conf);
    job.addCacheFile(firstCacheFile.toUri());
    job.addCacheFile(relativePath.toUri());
    jobConf = job.getConfiguration();
    statCache.clear();

    ClientDistributedCacheManager.determineCacheVisibilities(jobConf,
        statCache);
    // We use get() instead of getBoolean() so we can tell the difference
    // between wrong and missing
    assertEquals("The file paths were found to be publicly visible "
        + "even though the parent directory is not publicly accessible",
        "false,false", jobConf.get(MRJobConfig.CACHE_FILE_VISIBILITIES));
    checkCacheEntries(statCache, qualifiedParent,
        firstCacheFile, relativePath);

    job = Job.getInstance(conf);
    job.addCacheFile(wildcardPath.toUri());
    jobConf = job.getConfiguration();
    statCache.clear();

    ClientDistributedCacheManager.determineCacheVisibilities(jobConf,
        statCache);
    // We use get() instead of getBoolean() so we can tell the difference
    // between wrong and missing
    assertEquals("The file path was found to be publicly visible "
        + "even though the parent directory is not publicly accessible",
        "false", jobConf.get(MRJobConfig.CACHE_FILE_VISIBILITIES));
    checkCacheEntries(statCache, qualifiedParent, wildcardPath.getParent());
  }

  /**
   * Validate that the file status cache contains all and only entries for a
   * given set of paths up to a common parent.
   *
   * @param statCache the cache
   * @param top the common parent at which to stop digging
   * @param paths the paths to compare against the cache
   */
  private void checkCacheEntries(Map<URI, FileStatus> statCache, Path top,
      Path... paths) {
    Set<URI> expected = new HashSet<>();

    for (Path path : paths) {
      Path p = fs.makeQualified(path);

      while (!p.isRoot() && !p.equals(top)) {
        expected.add(p.toUri());
        p = p.getParent();
      }

      expected.add(p.toUri());
    }

    Set<URI> uris = statCache.keySet();
    Set<URI> missing = new HashSet<>(uris);
    Set<URI> extra = new HashSet<>(expected);

    missing.removeAll(expected);
    extra.removeAll(uris);

    assertTrue("File status cache does not contain an entries for " + missing,
        missing.isEmpty());
    assertTrue("File status cache contains extra extries: " + extra,
        extra.isEmpty());
  }

  @SuppressWarnings("deprecation")
  void createTempFile(Path p, Configuration conf) throws IOException {
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, p,
                                         Text.class, Text.class,
                                         CompressionType.NONE);
      writer.append(new Text("text"), new Text("moretext"));
    } catch(Exception e) {
      throw new IOException(e.getLocalizedMessage());
    } finally {
      if (writer != null) {
        writer.close();
      }
      writer = null;
    }
    LOG.info("created: " + p);
  }
}
