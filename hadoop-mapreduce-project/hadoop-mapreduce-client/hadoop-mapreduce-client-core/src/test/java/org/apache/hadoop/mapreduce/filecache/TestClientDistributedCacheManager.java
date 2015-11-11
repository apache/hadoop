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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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

  private FileSystem fs;
  private Path firstCacheFile;
  private Path secondCacheFile;
  private Path thirdCacheFile;
  private Configuration conf;
  
  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    fs = FileSystem.get(conf);
    firstCacheFile = new Path(TEST_ROOT_DIR, "firstcachefile");
    secondCacheFile = new Path(TEST_ROOT_DIR, "secondcachefile");
    thirdCacheFile = new Path(TEST_VISIBILITY_CHILD_DIR,"thirdCachefile");
    createTempFile(firstCacheFile, conf);
    createTempFile(secondCacheFile, conf);
    createTempFile(thirdCacheFile, conf);
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
    
    Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
    ClientDistributedCacheManager.determineTimestamps(jobConf, statCache);
    
    FileStatus firstStatus = statCache.get(firstCacheFile.toUri());
    FileStatus secondStatus = statCache.get(secondCacheFile.toUri());
    
    Assert.assertNotNull(firstStatus);
    Assert.assertNotNull(secondStatus);
    Assert.assertEquals(2, statCache.size());
    String expected = firstStatus.getModificationTime() + ","
        + secondStatus.getModificationTime();
    Assert.assertEquals(expected, jobConf.get(MRJobConfig.CACHE_FILE_TIMESTAMPS));
  }
  
  @Test
  public void testDetermineCacheVisibilities() throws IOException {
    fs.setWorkingDirectory(TEST_VISIBILITY_CHILD_DIR);
    fs.setPermission(TEST_VISIBILITY_CHILD_DIR,
        new FsPermission((short)00777));
    fs.setPermission(TEST_VISIBILITY_PARENT_DIR,
        new FsPermission((short)00700));
    Job job = Job.getInstance(conf);
    Path relativePath = new Path("thirdCachefile");
    job.addCacheFile(relativePath.toUri());
    Configuration jobConf = job.getConfiguration();

    Map<URI, FileStatus> statCache = new HashMap<URI, FileStatus>();
    ClientDistributedCacheManager.
        determineCacheVisibilities(jobConf, statCache);
    Assert.assertFalse(jobConf.
               getBoolean(MRJobConfig.CACHE_FILE_VISIBILITIES,true));
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
