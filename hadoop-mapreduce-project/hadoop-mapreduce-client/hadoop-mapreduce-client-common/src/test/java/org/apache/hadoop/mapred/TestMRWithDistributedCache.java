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
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Tests the use of the
 * {@link org.apache.hadoop.mapreduce.filecache.DistributedCache} within the
 * full MR flow as well as the LocalJobRunner. This ought to be part of the
 * filecache package, but that package is not currently in mapred, so cannot
 * depend on MR for testing.
 * 
 * We use the distributed.* namespace for temporary files.
 * 
 * See {@link TestMiniMRLocalFS}, {@link TestMiniMRDFSCaching}, and
 * {@link MRCaching} for other tests that test the distributed cache.
 * 
 * This test is not fast: it uses MiniMRCluster.
 */
@SuppressWarnings("deprecation")
public class TestMRWithDistributedCache {
  private static Path TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"));
  private static File symlinkFile = new File("distributed.first.symlink");
  private static File expectedAbsentSymlinkFile =
    new File("distributed.second.jar");
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMRWithDistributedCache.class);
  
  private static class DistributedCacheChecker {

    public void setup(TaskInputOutputContext<?, ?, ?, ?> context)
        throws IOException {
      Configuration conf = context.getConfiguration();
      Path[] localFiles = context.getLocalCacheFiles();
      URI[] files = context.getCacheFiles();
      Path[] localArchives = context.getLocalCacheArchives();
      URI[] archives = context.getCacheArchives();
      FileSystem fs = LocalFileSystem.get(conf);

      // Check that 2 files and 2 archives are present
      Assert.assertEquals(2, localFiles.length);
      Assert.assertEquals(2, localArchives.length);
      Assert.assertEquals(2, files.length);
      Assert.assertEquals(2, archives.length);

      // Check the file name
      Assert.assertTrue(files[0].getPath().endsWith("distributed.first"));
      Assert.assertTrue(files[1].getPath().endsWith("distributed.second.jar"));
      
      // Check lengths of the files
      Assert.assertEquals(1, fs.getFileStatus(localFiles[0]).getLen());
      Assert.assertTrue(fs.getFileStatus(localFiles[1]).getLen() > 1);

      // Check extraction of the archive
      Assert.assertTrue(fs.exists(new Path(localArchives[0],
          "distributed.jar.inside3")));
      Assert.assertTrue(fs.exists(new Path(localArchives[1],
          "distributed.jar.inside4")));

      // Check the class loaders
      LOG.info("Java Classpath: " + System.getProperty("java.class.path"));
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      // Both the file and the archive were added to classpath, so both
      // should be reachable via the class loader.
      Assert.assertNotNull(cl.getResource("distributed.jar.inside2"));
      Assert.assertNotNull(cl.getResource("distributed.jar.inside3"));
      Assert.assertNull(cl.getResource("distributed.jar.inside4"));

      // Check that the symlink for the renaming was created in the cwd;
      Assert.assertTrue("symlink distributed.first.symlink doesn't exist",
          symlinkFile.exists());
      Assert.assertEquals("symlink distributed.first.symlink length not 1", 1,
          symlinkFile.length());
      
      //This last one is a difference between MRv2 and MRv1
      Assert.assertTrue("second file should be symlinked too",
          expectedAbsentSymlinkFile.exists());
    }

  }

  public static class DistributedCacheCheckerMapper extends
      Mapper<LongWritable, Text, NullWritable, NullWritable> {

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      new DistributedCacheChecker().setup(context);
    }
  }

  public static class DistributedCacheCheckerReducer extends
      Reducer<LongWritable, Text, NullWritable, NullWritable> {

    @Override
    public void setup(Context context) throws IOException {
      new DistributedCacheChecker().setup(context);
    }
  }

  private void testWithConf(Configuration conf) throws IOException,
      InterruptedException, ClassNotFoundException, URISyntaxException {
    // Create a temporary file of length 1.
    Path first = createTempFile("distributed.first", "x");
    // Create two jars with a single file inside them.
    Path second =
        makeJar(new Path(TEST_ROOT_DIR, "distributed.second.jar"), 2);
    Path third =
        makeJar(new Path(TEST_ROOT_DIR, "distributed.third.jar"), 3);
    Path fourth =
        makeJar(new Path(TEST_ROOT_DIR, "distributed.fourth.jar"), 4);


    Job job = Job.getInstance(conf);
    job.setMapperClass(DistributedCacheCheckerMapper.class);
    job.setReducerClass(DistributedCacheCheckerReducer.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    FileInputFormat.setInputPaths(job, first);
    // Creates the Job Configuration
    job.addCacheFile(
      new URI(first.toUri().toString() + "#distributed.first.symlink"));
    job.addFileToClassPath(second);
    job.addArchiveToClassPath(third);
    job.addCacheArchive(fourth.toUri());
    job.setMaxMapAttempts(1); // speed up failures

    job.submit();
    assertTrue(job.waitForCompletion(false));
  }

  /** Tests using the local job runner. */
  @Test
  public void testLocalJobRunner() throws Exception {
    symlinkFile.delete(); // ensure symlink is not present (e.g. if test is
                          // killed part way through)
    
    Configuration c = new Configuration();
    c.set(JTConfig.JT_IPC_ADDRESS, "local");
    c.set("fs.defaultFS", "file:///");
    testWithConf(c);
    
    assertFalse("Symlink not removed by local job runner",
            // Symlink target will have gone so can't use File.exists()
            Arrays.asList(new File(".").list()).contains(symlinkFile.getName()));
  }

  private Path createTempFile(String filename, String contents)
      throws IOException {
    Path path = new Path(TEST_ROOT_DIR, filename);
    FSDataOutputStream os = localFs.create(path);
    os.writeBytes(contents);
    os.close();
    return path;
  }

  private Path makeJar(Path p, int index) throws FileNotFoundException,
      IOException {
    FileOutputStream fos = new FileOutputStream(new File(p.toString()));
    JarOutputStream jos = new JarOutputStream(fos);
    ZipEntry ze = new ZipEntry("distributed.jar.inside" + index);
    jos.putNextEntry(ze);
    jos.write(("inside the jar!" + index).getBytes());
    jos.closeEntry();
    jos.close();
    return p;
  }

  @Test (timeout = 10000)
  public void testDeprecatedFunctions() throws Exception {
    DistributedCache.addLocalArchives(conf, "Test Local Archives 1");
    Assert.assertEquals("Test Local Archives 1",
        conf.get(DistributedCache.CACHE_LOCALARCHIVES));
    Assert.assertEquals(1,
        DistributedCache.getLocalCacheArchives(conf).length);
    Assert.assertEquals("Test Local Archives 1",
        DistributedCache.getLocalCacheArchives(conf)[0].getName());
    DistributedCache.addLocalArchives(conf, "Test Local Archives 2");
    Assert.assertEquals("Test Local Archives 1,Test Local Archives 2",
        conf.get(DistributedCache.CACHE_LOCALARCHIVES));
    Assert.assertEquals(2,
        DistributedCache.getLocalCacheArchives(conf).length);
    Assert.assertEquals("Test Local Archives 2",
        DistributedCache.getLocalCacheArchives(conf)[1].getName());
    DistributedCache.setLocalArchives(conf, "Test Local Archives 3");
    Assert.assertEquals("Test Local Archives 3",
        conf.get(DistributedCache.CACHE_LOCALARCHIVES));
    Assert.assertEquals(1,
        DistributedCache.getLocalCacheArchives(conf).length);
    Assert.assertEquals("Test Local Archives 3",
        DistributedCache.getLocalCacheArchives(conf)[0].getName());

    DistributedCache.addLocalFiles(conf, "Test Local Files 1");
    Assert.assertEquals("Test Local Files 1",
        conf.get(DistributedCache.CACHE_LOCALFILES));
    Assert.assertEquals(1,
        DistributedCache.getLocalCacheFiles(conf).length);
    Assert.assertEquals("Test Local Files 1",
        DistributedCache.getLocalCacheFiles(conf)[0].getName());
    DistributedCache.addLocalFiles(conf, "Test Local Files 2");
    Assert.assertEquals("Test Local Files 1,Test Local Files 2",
        conf.get(DistributedCache.CACHE_LOCALFILES));
    Assert.assertEquals(2,
        DistributedCache.getLocalCacheFiles(conf).length);
    Assert.assertEquals("Test Local Files 2",
        DistributedCache.getLocalCacheFiles(conf)[1].getName());
    DistributedCache.setLocalFiles(conf, "Test Local Files 3");
    Assert.assertEquals("Test Local Files 3",
        conf.get(DistributedCache.CACHE_LOCALFILES));
    Assert.assertEquals(1,
        DistributedCache.getLocalCacheFiles(conf).length);
    Assert.assertEquals("Test Local Files 3",
        DistributedCache.getLocalCacheFiles(conf)[0].getName());

    DistributedCache.setArchiveTimestamps(conf, "1234567890");
    Assert.assertEquals(1234567890,
        conf.getLong(DistributedCache.CACHE_ARCHIVES_TIMESTAMPS, 0));
    Assert.assertEquals(1,
        DistributedCache.getArchiveTimestamps(conf).length);
    Assert.assertEquals(1234567890,
        DistributedCache.getArchiveTimestamps(conf)[0]);
    DistributedCache.setFileTimestamps(conf, "1234567890");
    Assert.assertEquals(1234567890,
        conf.getLong(DistributedCache.CACHE_FILES_TIMESTAMPS, 0));
    Assert.assertEquals(1,
        DistributedCache.getFileTimestamps(conf).length);
    Assert.assertEquals(1234567890,
        DistributedCache.getFileTimestamps(conf)[0]);

    DistributedCache.createAllSymlink(conf, new File("Test Job Cache Dir"),
        new File("Test Work Dir"));
    Assert.assertNull(conf.get(DistributedCache.CACHE_SYMLINK));
    Assert.assertTrue(DistributedCache.getSymlink(conf));

    Assert.assertTrue(symlinkFile.createNewFile());
    FileStatus fileStatus =
        DistributedCache.getFileStatus(conf, symlinkFile.toURI());
    Assert.assertNotNull(fileStatus);
    Assert.assertEquals(fileStatus.getModificationTime(),
        DistributedCache.getTimestamp(conf, symlinkFile.toURI()));
    Assert.assertTrue(symlinkFile.delete());

    DistributedCache.addCacheArchive(symlinkFile.toURI(), conf);
    Assert.assertEquals(symlinkFile.toURI().toString(),
        conf.get(DistributedCache.CACHE_ARCHIVES));
    Assert.assertEquals(1, DistributedCache.getCacheArchives(conf).length);
    Assert.assertEquals(symlinkFile.toURI(),
        DistributedCache.getCacheArchives(conf)[0]);

    DistributedCache.addCacheFile(symlinkFile.toURI(), conf);
    Assert.assertEquals(symlinkFile.toURI().toString(),
        conf.get(DistributedCache.CACHE_FILES));
    Assert.assertEquals(1, DistributedCache.getCacheFiles(conf).length);
    Assert.assertEquals(symlinkFile.toURI(),
        DistributedCache.getCacheFiles(conf)[0]);
  }
}
