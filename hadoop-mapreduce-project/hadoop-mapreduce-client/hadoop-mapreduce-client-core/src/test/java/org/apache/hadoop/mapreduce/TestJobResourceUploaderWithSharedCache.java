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

package org.apache.hadoop.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.SharedCacheClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the JobResourceUploader class with the shared cache.
 */
public class TestJobResourceUploaderWithSharedCache {
  protected static final Logger LOG = LoggerFactory
      .getLogger(TestJobResourceUploaderWithSharedCache.class);
  private static MiniDFSCluster dfs;
  private static FileSystem localFs;
  private static FileSystem remoteFs;
  private static Configuration conf = new Configuration();
  private static Path testRootDir;
  private static Path remoteStagingDir =
      new Path(MRJobConfig.DEFAULT_MR_AM_STAGING_DIR);
  private String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";

  @Before
  public void cleanup() throws Exception {
    remoteFs.delete(remoteStagingDir, true);
  }

  @BeforeClass
  public static void setup() throws IOException {
    // create configuration, dfs, file system
    localFs = FileSystem.getLocal(conf);
    testRootDir =
        new Path("target",
            TestJobResourceUploaderWithSharedCache.class.getName() + "-tmpDir")
            .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    dfs = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    remoteFs = dfs.getFileSystem();
  }

  @AfterClass
  public static void tearDown() {
    try {
      if (localFs != null) {
        localFs.close();
      }
      if (remoteFs != null) {
        remoteFs.close();
      }
      if (dfs != null) {
        dfs.shutdown();
      }
    } catch (IOException ioe) {
      LOG.info("IO exception in closing file system");
      ioe.printStackTrace();
    }
  }

  private class MyFileUploader extends JobResourceUploader {
    // The mocked SharedCacheClient that will be fed into the FileUploader
    private SharedCacheClient mockscClient = mock(SharedCacheClient.class);
    // A real client for checksum calculation
    private SharedCacheClient scClient = SharedCacheClient
        .createSharedCacheClient();

    MyFileUploader(FileSystem submitFs, Configuration conf)
        throws IOException {
      super(submitFs, false);
      // Initialize the real client, but don't start it. We don't need or want
      // to create an actual proxy because we only use this for mocking out the
      // getFileChecksum method.
      scClient.init(conf);
      when(mockscClient.getFileChecksum(any(Path.class))).thenAnswer(
          new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
              Path file = (Path) invocation.getArguments()[0];
              // Use the real scClient to generate the checksum. We use an
              // answer/mock combination to avoid having to spy on a real
              // SharedCacheClient object.
              return scClient.getFileChecksum(file);
            }
          });
    }

    // This method is to prime the mock client with the correct checksum, so it
    // looks like a given resource is present in the shared cache.
    public void mockFileInSharedCache(Path localFile, URL remoteFile)
        throws YarnException, IOException {
      // when the resource is referenced, simply return the remote path to the
      // caller
      when(mockscClient.use(any(ApplicationId.class),
          eq(scClient.getFileChecksum(localFile)))).thenReturn(remoteFile);
    }

    @Override
    protected SharedCacheClient createSharedCacheClient(Configuration c) {
      // Feed the mocked SharedCacheClient into the FileUploader logic
      return mockscClient;
    }
  }

  @Test
  public void testSharedCacheDisabled() throws Exception {
    JobConf jobConf = createJobConf();
    Job job = new Job(jobConf);
    job.setJobID(new JobID("567789", 1));

    // shared cache is disabled by default
    uploadFilesToRemoteFS(job, jobConf, 0, 0, 0, false);

  }

  @Test
  public void testSharedCacheEnabled() throws Exception {
    JobConf jobConf = createJobConf();
    jobConf.set(MRJobConfig.SHARED_CACHE_MODE, "enabled");
    Job job = new Job(jobConf);
    job.setJobID(new JobID("567789", 1));

    // shared cache is enabled for every file type
    // the # of times SharedCacheClient.use is called should ==
    // total # of files/libjars/archive/jobjar
    uploadFilesToRemoteFS(job, jobConf, 8, 3, 2, false);
  }

  @Test
  public void testSharedCacheEnabledWithJobJarInSharedCache()
      throws Exception {
    JobConf jobConf = createJobConf();
    jobConf.set(MRJobConfig.SHARED_CACHE_MODE, "enabled");
    Job job = new Job(jobConf);
    job.setJobID(new JobID("567789", 1));

    // shared cache is enabled for every file type
    // the # of times SharedCacheClient.use is called should ==
    // total # of files/libjars/archive/jobjar
    uploadFilesToRemoteFS(job, jobConf, 8, 3, 2, true);
  }

  @Test
  public void testSharedCacheArchivesAndLibjarsEnabled() throws Exception {
    JobConf jobConf = createJobConf();
    jobConf.set(MRJobConfig.SHARED_CACHE_MODE, "archives,libjars");
    Job job = new Job(jobConf);
    job.setJobID(new JobID("567789", 1));

    // shared cache is enabled for archives and libjars type
    // the # of times SharedCacheClient.use is called should ==
    // total # of libjars and archives
    uploadFilesToRemoteFS(job, jobConf, 5, 1, 2, true);
  }

  private JobConf createJobConf() {
    JobConf jobConf = new JobConf();
    jobConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    jobConf.setBoolean(YarnConfiguration.SHARED_CACHE_ENABLED, true);

    jobConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, remoteFs.getUri()
        .toString());
    return jobConf;
  }

  private Path copyToRemote(Path jar) throws IOException {
    Path remoteFile = new Path("/tmp", jar.getName());
    remoteFs.copyFromLocalFile(jar, remoteFile);
    return remoteFile;
  }

  private void makeJarAvailableInSharedCache(Path jar,
      MyFileUploader fileUploader) throws YarnException, IOException {
    // copy file to remote file system
    Path remoteFile = copyToRemote(jar);
    // prime mocking so that it looks like this file is in the shared cache
    fileUploader.mockFileInSharedCache(jar, URL.fromPath(remoteFile));
  }

  private void uploadFilesToRemoteFS(Job job, JobConf jobConf,
      int useCallCountExpected,
      int numOfFilesShouldBeUploadedToSharedCacheExpected,
      int numOfArchivesShouldBeUploadedToSharedCacheExpected,
      boolean jobJarInSharedCacheBeforeUpload) throws Exception {
    MyFileUploader fileUploader = new MyFileUploader(remoteFs, jobConf);
    SharedCacheConfig sharedCacheConfig = new SharedCacheConfig();
    sharedCacheConfig.init(jobConf);

    Path firstFile = createTempFile("first-input-file", "x");
    Path secondFile = createTempFile("second-input-file", "xx");

    // Add files to job conf via distributed cache API as well as command line
    boolean fileAdded = Job.addFileToSharedCache(firstFile.toUri(), jobConf);
    assertEquals(sharedCacheConfig.isSharedCacheFilesEnabled(), fileAdded);
    if (!fileAdded) {
      Path remoteFile = copyToRemote(firstFile);
      job.addCacheFile(remoteFile.toUri());
    }
    jobConf.set("tmpfiles", secondFile.toString());

    // Create jars with a single file inside them.
    Path firstJar = makeJar(new Path(testRootDir, "distributed.first.jar"), 1);
    Path secondJar =
        makeJar(new Path(testRootDir, "distributed.second.jar"), 2);

    // Verify duplicated contents can be handled properly.
    Path thirdJar = new Path(testRootDir, "distributed.third.jar");
    localFs.copyFromLocalFile(secondJar, thirdJar);

    // make secondJar cache available
    makeJarAvailableInSharedCache(secondJar, fileUploader);

    // Add libjars to job conf via distributed cache API as well as command
    // line
    boolean libjarAdded =
        Job.addFileToSharedCacheAndClasspath(firstJar.toUri(), jobConf);
    assertEquals(sharedCacheConfig.isSharedCacheLibjarsEnabled(), libjarAdded);
    if (!libjarAdded) {
      Path remoteJar = copyToRemote(firstJar);
      job.addFileToClassPath(remoteJar);
    }

    jobConf.set("tmpjars", secondJar.toString() + "," + thirdJar.toString());

    Path firstArchive = makeArchive("first-archive.zip", "first-file");
    Path secondArchive = makeArchive("second-archive.zip", "second-file");

    // Add archives to job conf via distributed cache API as well as command
    // line
    boolean archiveAdded =
        Job.addArchiveToSharedCache(firstArchive.toUri(), jobConf);
    assertEquals(sharedCacheConfig.isSharedCacheArchivesEnabled(),
        archiveAdded);
    if (!archiveAdded) {
      Path remoteArchive = copyToRemote(firstArchive);
      job.addCacheArchive(remoteArchive.toUri());
    }

    jobConf.set("tmparchives", secondArchive.toString());

    // Add job jar to job conf
    Path jobJar = makeJar(new Path(testRootDir, "test-job.jar"), 4);
    if (jobJarInSharedCacheBeforeUpload) {
      makeJarAvailableInSharedCache(jobJar, fileUploader);
    }
    jobConf.setJar(jobJar.toString());

    fileUploader.uploadResources(job, remoteStagingDir);

    verify(fileUploader.mockscClient, times(useCallCountExpected)).use(
        any(ApplicationId.class), anyString());

    int numOfFilesShouldBeUploadedToSharedCache = 0;
    Map<String, Boolean> filesSharedCacheUploadPolicies =
        Job.getFileSharedCacheUploadPolicies(jobConf);
    for (Boolean policy : filesSharedCacheUploadPolicies.values()) {
      if (policy) {
        numOfFilesShouldBeUploadedToSharedCache++;
      }
    }
    assertEquals(numOfFilesShouldBeUploadedToSharedCacheExpected,
        numOfFilesShouldBeUploadedToSharedCache);

    int numOfArchivesShouldBeUploadedToSharedCache = 0;
    Map<String, Boolean> archivesSharedCacheUploadPolicies =
        Job.getArchiveSharedCacheUploadPolicies(jobConf);
    for (Boolean policy : archivesSharedCacheUploadPolicies.values()) {
      if (policy) {
        numOfArchivesShouldBeUploadedToSharedCache++;
      }
    }
    assertEquals(numOfArchivesShouldBeUploadedToSharedCacheExpected,
        numOfArchivesShouldBeUploadedToSharedCache);
  }


  private Path createTempFile(String filename, String contents)
      throws IOException {
    Path path = new Path(testRootDir, filename);
    FSDataOutputStream os = localFs.create(path);
    os.writeBytes(contents);
    os.close();
    localFs.setPermission(path, new FsPermission("700"));
    return path;
  }

  private Path makeJar(Path p, int index) throws FileNotFoundException,
      IOException {
    FileOutputStream fos =
        new FileOutputStream(new File(p.toUri().getPath()));
    JarOutputStream jos = new JarOutputStream(fos);
    ZipEntry ze = new ZipEntry("distributed.jar.inside" + index);
    jos.putNextEntry(ze);
    jos.write(("inside the jar!" + index).getBytes());
    jos.closeEntry();
    jos.close();
    localFs.setPermission(p, new FsPermission("700"));
    return p;
  }

  private Path makeArchive(String archiveFile, String filename)
      throws Exception {
    Path archive = new Path(testRootDir, archiveFile);
    Path file = new Path(testRootDir, filename);
    DataOutputStream out = localFs.create(archive);
    ZipOutputStream zos = new ZipOutputStream(out);
    ZipEntry ze = new ZipEntry(file.toString());
    zos.putNextEntry(ze);
    zos.write(input.getBytes("UTF-8"));
    zos.closeEntry();
    zos.close();
    return archive;
  }
}
