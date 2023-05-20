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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.impl.FutureDataInputStreamBuilderImpl;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test the LocalDistributedCacheManager using mocking.
 * This suite is brittle to changes in the class under test.
 */
@SuppressWarnings("deprecation")
public class TestLocalDistributedCacheManager {

  private static final byte[] TEST_DATA = "This is a test file\n".getBytes();

  private static FileSystem mockfs;

  public static class MockFileSystem extends FilterFileSystem {
    public MockFileSystem() {
      super(mockfs);
    }
  }

  private File localDir;

  /**
   * Recursive delete of a path.
   * For safety, paths of length under 5 are rejected.
   * @param file path to delete.
   * @throws IOException never, it is just "a dummy in the method signature"
   * @throws IllegalArgumentException path too short
   * @throws RuntimeException File.delete() failed.
   */
  private static void delete(File file) throws IOException {
    if (file.getAbsolutePath().length() < 5) {
      throw new IllegalArgumentException(
          "Path [" + file + "] is too short, not deleting");
    }
    if (file.exists()) {
      if (file.isDirectory()) {
        File[] children = file.listFiles();
        if (children != null) {
          for (File child : children) {
            delete(child);
          }
        }
      }
      if (!file.delete()) {
        throw new RuntimeException(
          "Could not delete path [" + file + "]");
      }
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    mockfs = mock(FileSystem.class);
    localDir = new File(System.getProperty("test.build.dir", "target/test-dir"),
        TestLocalDistributedCacheManager.class.getName());
    delete(localDir);
    localDir.mkdirs();
  }

  @AfterEach
  public void cleanup() throws Exception {
    delete(localDir);
  }

  /**
   * Mock input stream based on a byte array so that it can be used by a
   * FSDataInputStream.
   */
  private static final class MockInputStream extends ByteArrayInputStream
      implements Seekable, PositionedReadable {
    private MockInputStream(byte[] buf) {
      super(buf);
    }

    // empty implementation for unused methods
    public int read(long position, byte[] buffer, int offset, int length) { return -1; }
    public void readFully(long position, byte[] buffer, int offset, int length) {}
    public void readFully(long position, byte[] buffer) {}
    public void seek(long position) {}
    public long getPos() { return 0; }
    public boolean seekToNewSource(long targetPos) { return false; }
  }

  @Test
  public void testDownload() throws Exception {
    JobID jobId = new JobID();
    JobConf conf = new JobConf();
    conf.setClass("fs.mock.impl", MockFileSystem.class, FileSystem.class);

    URI mockBase = new URI("mock://test-nn1/");
    when(mockfs.getUri()).thenReturn(mockBase);
    Path working = new Path("mock://test-nn1/user/me/");
    when(mockfs.getWorkingDirectory()).thenReturn(working);
    when(mockfs.resolvePath(any(Path.class))).thenAnswer(
        (Answer<Path>) args -> (Path) args.getArguments()[0]);

    final URI file = new URI("mock://test-nn1/user/me/file.txt#link");
    final Path filePath = new Path(file);
    File link = new File("link");

    // return a filestatus for the file "*/file.txt"; raise FNFE for anything else
    when(mockfs.getFileStatus(any(Path.class))).thenAnswer(new Answer<FileStatus>() {
      @Override
      public FileStatus answer(InvocationOnMock args) throws Throwable {
        Path p = (Path) args.getArguments()[0];
        if ("file.txt".equals(p.getName())) {
          return createMockTestFileStatus(filePath);
        }  else {
          throw notMocked(p);
        }
      }
    });

    when(mockfs.getConf()).thenReturn(conf);
    final FSDataInputStream in =
        new FSDataInputStream(new MockInputStream(TEST_DATA));

    // file.txt: return an openfile builder which will eventually return the data,
    // anything else: FNFE
    when(mockfs.openFile(any(Path.class))).thenAnswer(
        (Answer<FutureDataInputStreamBuilder>) args -> {
          Path src = (Path) args.getArguments()[0];
          if ("file.txt".equals(src.getName())) {
            return new MockOpenFileBuilder(mockfs, src,
                () -> CompletableFuture.completedFuture(in));
          } else {
            throw notMocked(src);
          }
        });

    Job.addCacheFile(file, conf);
    Map<String, Boolean> policies = new HashMap<>();
    policies.put(file.toString(), true);
    Job.setFileSharedCacheUploadPolicies(conf, policies);
    conf.set(MRJobConfig.CACHE_FILE_TIMESTAMPS, "101");
    conf.set(MRJobConfig.CACHE_FILES_SIZES, "201");
    conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, "false");
    conf.set(MRConfig.LOCAL_DIR, localDir.getAbsolutePath());
    LocalDistributedCacheManager manager = new LocalDistributedCacheManager();
    try {
      manager.setup(conf, jobId);
      assertTrue(link.exists());
    } finally {
      manager.close();
    }
    assertFalse(link.exists());
  }

  /**
   * This test case sets the mock FS to raise FNFE
   * on any getFileStatus/openFile calls.
   * If the manager successfully starts up, it means that
   * no files were probed for/opened.
   */
  @Test
  public void testEmptyDownload() throws Exception {
    JobID jobId = new JobID();
    JobConf conf = new JobConf();
    conf.setClass("fs.mock.impl", MockFileSystem.class, FileSystem.class);

    URI mockBase = new URI("mock://test-nn1/");
    when(mockfs.getUri()).thenReturn(mockBase);
    Path working = new Path("mock://test-nn1/user/me/");
    when(mockfs.getWorkingDirectory()).thenReturn(working);
    when(mockfs.resolvePath(any(Path.class))).thenAnswer(
        (Answer<Path>) args -> (Path) args.getArguments()[0]);

    when(mockfs.getFileStatus(any(Path.class))).thenAnswer(
        (Answer<FileStatus>) args -> {
          Path p = (Path) args.getArguments()[0];
          throw notMocked(p);
        });

    when(mockfs.getConf()).thenReturn(conf);
    when(mockfs.openFile(any(Path.class))).thenAnswer(
        (Answer<FutureDataInputStreamBuilder>) args -> {
          Path src = (Path) args.getArguments()[0];
          throw notMocked(src);
        });
    conf.set(MRJobConfig.CACHE_FILES, "");
    conf.set(MRConfig.LOCAL_DIR, localDir.getAbsolutePath());
    LocalDistributedCacheManager manager = new LocalDistributedCacheManager();
    try {
      manager.setup(conf, jobId);
    } finally {
      manager.close();
    }
  }


  /**
   * The same file can be added to the cache twice.
   */
  @Test
  public void testDuplicateDownload() throws Exception {
    JobID jobId = new JobID();
    JobConf conf = new JobConf();
    conf.setClass("fs.mock.impl", MockFileSystem.class, FileSystem.class);

    URI mockBase = new URI("mock://test-nn1/");
    when(mockfs.getUri()).thenReturn(mockBase);
    Path working = new Path("mock://test-nn1/user/me/");
    when(mockfs.getWorkingDirectory()).thenReturn(working);
    when(mockfs.resolvePath(any(Path.class))).thenAnswer(
        (Answer<Path>) args -> (Path) args.getArguments()[0]);

    final URI file = new URI("mock://test-nn1/user/me/file.txt#link");
    final Path filePath = new Path(file);
    File link = new File("link");

    when(mockfs.getFileStatus(any(Path.class))).thenAnswer(new Answer<FileStatus>() {
      @Override
      public FileStatus answer(InvocationOnMock args) throws Throwable {
        Path p = (Path) args.getArguments()[0];
        if ("file.txt".equals(p.getName())) {
          return createMockTestFileStatus(filePath);
        }  else {
          throw notMocked(p);
        }
      }
    });

    when(mockfs.getConf()).thenReturn(conf);
    final FSDataInputStream in =
        new FSDataInputStream(new MockInputStream(TEST_DATA));
    when(mockfs.openFile(any(Path.class))).thenAnswer(
        (Answer<FutureDataInputStreamBuilder>) args -> {
          Path src = (Path) args.getArguments()[0];
          if ("file.txt".equals(src.getName())) {
            return new MockOpenFileBuilder(mockfs, src,
                () -> CompletableFuture.completedFuture(in));
          } else {
            throw notMocked(src);
          }
        });

    Job.addCacheFile(file, conf);
    Job.addCacheFile(file, conf);
    Map<String, Boolean> policies = new HashMap<>();
    policies.put(file.toString(), true);
    Job.setFileSharedCacheUploadPolicies(conf, policies);
    conf.set(MRJobConfig.CACHE_FILE_TIMESTAMPS, "101,101");
    conf.set(MRJobConfig.CACHE_FILES_SIZES, "201,201");
    conf.set(MRJobConfig.CACHE_FILE_VISIBILITIES, "false,false");
    conf.set(MRConfig.LOCAL_DIR, localDir.getAbsolutePath());
    LocalDistributedCacheManager manager = new LocalDistributedCacheManager();
    try {
      manager.setup(conf, jobId);
      assertTrue(link.exists());
    } finally {
      manager.close();
    }
    assertFalse(link.exists());
  }

  /**
   * This test tries to replicate the issue with the previous version of
   * {@link LocalDistributedCacheManager} when the resulting timestamp is
   * identical as that in another process.  Unfortunately, it is difficult
   * to mimic such behavior in a single process unit test.  And mocking
   * the unique id (timestamp previously, UUID otherwise) won't prove the
   * validity of one approach over the other.
   */
  @Test
  public void testMultipleCacheSetup() throws Exception {
    JobID jobId = new JobID();
    JobConf conf = new JobConf();
    LocalDistributedCacheManager manager = new LocalDistributedCacheManager();

    final int threadCount = 10;
    final CyclicBarrier barrier = new CyclicBarrier(threadCount);

    List<Callable<Void>> setupCallable = new ArrayList<>();
    for (int i = 0; i < threadCount; ++i) {
      setupCallable.add(() -> {
        barrier.await();
        manager.setup(conf, jobId);
        return null;
      });
    }

    ExecutorService ePool = Executors.newFixedThreadPool(threadCount);
    try {
      for (Future<Void> future : ePool.invokeAll(setupCallable)) {
        future.get();
      }
    } finally {
      ePool.shutdown();
      manager.close();
    }
  }

  /**
   * Create test file status using test data as the length.
   * @param filePath path to the file
   * @return a file status.
   */
  private FileStatus createMockTestFileStatus(final Path filePath) {
    return new FileStatus(TEST_DATA.length, false, 1, 500, 101, 101,
        FsPermission.getDefault(), "me", "me", filePath);
  }

  /**
   * Exception to throw on a not mocked path.
   * @return a FileNotFoundException
   */
  private FileNotFoundException notMocked(final Path p) {
    return new FileNotFoundException(p + " not supported by mocking");
  }

  /**
   * Openfile builder where the build operation is a l-expression
   * supplied in the constructor.
   */
  private static final class MockOpenFileBuilder extends
      FutureDataInputStreamBuilderImpl {

    /**
     * Operation to invoke to build the result.
     */
    private final CallableRaisingIOE<CompletableFuture<FSDataInputStream>>
        buildTheResult;

    /**
     * Create the builder. the FS and path must be non-null.
     * FileSystem.getConf() is the only method invoked of the FS by
     * the superclass.
     * @param fileSystem fs
     * @param path path to open
     * @param buildTheResult builder operation.
     */
    private MockOpenFileBuilder(final FileSystem fileSystem, Path path,
        final CallableRaisingIOE<CompletableFuture<FSDataInputStream>> buildTheResult) {
      super(fileSystem, path);
      this.buildTheResult = buildTheResult;
    }

    @Override
    public CompletableFuture<FSDataInputStream> build()
        throws IllegalArgumentException, UnsupportedOperationException,
               IOException {
      return buildTheResult.apply();
    }
  }

}
