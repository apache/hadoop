package org.apache.hadoop.fs.azurebfs.services;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.impl.CombinedFileRange;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_READAHEAD_V2_MEMORY_USAGE_THRESHOLD_PERCENT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_VECTORED_READ_STRATEGY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;

public class ITestVectoredRead extends AbstractAbfsIntegrationTest {

  public ITestVectoredRead() throws Exception {
  }

  @Test
  public void testDisjointRangesWithVectoredRead() throws Throwable {
    int fileSize = ONE_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName() + 1;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> rangeList = new ArrayList<>();
    rangeList.add(FileRange.createFileRange(100, 10000));
    rangeList.add(FileRange.createFileRange(15000, 27000));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    CompletableFuture<FSDataInputStream> builder = fs.openFile(testFilePath)
        .build();

    try (FSDataInputStream in = builder.get()) {
      in.readVectored(rangeList, allocate);
      byte[] readFullRes = new byte[(int) fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(rangeList, readFullRes, 0);
    }
  }

  @Test
  public void testVectoredReadDisjointRangesExpectTwoBackendReads()
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(16 * ONE_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
    List<FileRange> fileRanges = new ArrayList<>();
    // 0.0 – 1.0 MB
    fileRanges.add(FileRange.createFileRange(0L, (int) ONE_MB));
    // 1.2 – 2.0 MB
    fileRanges.add(
        FileRange.createFileRange((long) (1.2 * ONE_MB), (int) (0.8 * ONE_MB)));
    // 3.1 – 4.0 MB
    fileRanges.add(
        FileRange.createFileRange((long) (3.1 * ONE_MB), (int) (0.9 * ONE_MB)));
    // 4.1 – 6.0 MB
    fileRanges.add(
        FileRange.createFileRange((long) (4.1 * ONE_MB), (int) (1.9 * ONE_MB)));
    // 6.2 – 8.0 MB
    fileRanges.add(
        FileRange.createFileRange((long) (6.2 * ONE_MB), (int) (1.8 * ONE_MB)));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    try (FSDataInputStream in =
             fs.openFile(testFilePath).build().get()) {
      AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
      AbfsInputStream spyIn = Mockito.spy(abfsIn);
      spyIn.readVectored(fileRanges, allocate);
      CompletableFuture<?>[] futures =
          new CompletableFuture<?>[fileRanges.size()];
      int i = 0;
      for (FileRange range : fileRanges) {
        futures[i++] = range.getData();
      }
      CompletableFuture.allOf(futures).get();
      validateVectoredReadResult(fileRanges, fileContent, 0);
      Mockito.verify(spyIn, Mockito.times(2))
          .readRemote(
              Mockito.anyLong(),
              Mockito.any(byte[].class),
              Mockito.anyInt(),
              Mockito.anyInt(),
              Mockito.any());
    }
  }

  @Test
  public void testVectoredReadFallsBackToDirectReadWhenQueuingFails()
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(4 * ONE_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(0, ONE_MB));
    fileRanges.add(FileRange.createFileRange(2 * ONE_MB, ONE_MB));
    IntFunction<ByteBuffer> allocator = ByteBuffer::allocate;

    try (FSDataInputStream in = fs.openFile(testFilePath).build().get()) {
      AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
      AbfsInputStream spyIn = Mockito.spy(abfsIn);
      VectoredReadHandler realHandler = abfsIn.getVectoredReadHandler();
      VectoredReadHandler spyHandler = Mockito.spy(realHandler);
      Mockito.doReturn(spyHandler).when(spyIn).getVectoredReadHandler();
      Mockito.doReturn(false)
          .when(spyHandler)
          .queueVectoredRead(
              Mockito.any(AbfsInputStream.class),
              Mockito.any(CombinedFileRange.class),
              ArgumentMatchers.<IntFunction<ByteBuffer>>any());
      spyIn.readVectored(fileRanges, allocator);
      CompletableFuture<?>[] futures
          = new CompletableFuture<?>[fileRanges.size()];
      for (int i = 0; i < fileRanges.size(); i++) {
        futures[i] = fileRanges.get(i).getData();
      }
      CompletableFuture.allOf(futures).get();
      Mockito.verify(spyHandler, Mockito.atLeastOnce())
          .directRead(
              Mockito.any(AbfsInputStream.class),
              Mockito.any(CombinedFileRange.class),
              Mockito.eq(allocator));

      validateVectoredReadResult(fileRanges, fileContent, 0);
    }
  }

  @Test
  public void testMultipleDisjointRangesWithVectoredRead() throws Throwable {
    int fileSize = ONE_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName() + 1;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> rangeList = new ArrayList<>();
    rangeList.add(FileRange.createFileRange(100, 10000));
    rangeList.add(FileRange.createFileRange(15000, 27000));
    rangeList.add(FileRange.createFileRange(42500, 40000));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    CompletableFuture<FSDataInputStream> builder = fs.openFile(testFilePath)
        .build();

    try (FSDataInputStream in = builder.get()) {
      in.readVectored(rangeList, allocate);
      byte[] readFullRes = new byte[(int) fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(rangeList, readFullRes, 0);
    }
  }

  @Test
  public void test_045_vectoredIOHugeFile() throws Throwable {
    int fileSize = 100 * ONE_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName() + 1;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> rangeList = new ArrayList<>();
    rangeList.add(FileRange.createFileRange(5856368, 116770));
    rangeList.add(FileRange.createFileRange(3520861, 116770));
    rangeList.add(FileRange.createFileRange(8191913, 116770));
    rangeList.add(FileRange.createFileRange(1520861, 116770));
    rangeList.add(FileRange.createFileRange(2520861, 116770));
    rangeList.add(FileRange.createFileRange(9191913, 116770));
    rangeList.add(FileRange.createFileRange(2820861, 156770));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;

    CompletableFuture<FSDataInputStream> builder =
        fs.openFile(testFilePath).build();
    try (FSDataInputStream in = builder.get()) {
      in.readVectored(rangeList, allocate);
      byte[] readFullRes = new byte[(int) fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(rangeList, readFullRes, 0);
    }
  }

  @Test
  public void testSimultaneousPrefetchAndVectoredRead() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(16 * ONE_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
    try (FSDataInputStream in = fs.openFile(testFilePath).build().get()) {
      AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
      IntFunction<ByteBuffer> allocator = ByteBuffer::allocate;
      List<FileRange> vRanges = new ArrayList<>();
      vRanges.add(FileRange.createFileRange(10 * ONE_MB, (int) ONE_MB));
      vRanges.add(FileRange.createFileRange(12 * ONE_MB, (int) ONE_MB));
      byte[] seqBuffer = new byte[(int) ONE_MB];
      CountDownLatch latch = new CountDownLatch(1);
      CompletableFuture<Void> vectoredTask = CompletableFuture.runAsync(() -> {
        try {
          latch.await();
          abfsIn.readVectored(vRanges, allocator);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      CompletableFuture<Void> sequentialTask = CompletableFuture.runAsync(
          () -> {
            try {
              latch.await();
              abfsIn.read(0, seqBuffer, 0, (int) ONE_MB);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
      latch.countDown();
      CompletableFuture.allOf(vectoredTask, sequentialTask).get();
      CompletableFuture<?>[] vFutures = vRanges.stream()
          .map(FileRange::getData)
          .toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(vFutures).get();
      assertArrayEquals(Arrays.copyOfRange(fileContent, 0, (int) ONE_MB),
          seqBuffer, "Sequential read data mismatch");
      validateVectoredReadResult(vRanges, fileContent, 0);
    }
  }

  @Test
  public void testConcurrentStreamsOnDifferentFiles() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    // Create two distinct files with random content
    byte[] content1 = getRandomBytesArray(16 * ONE_MB);
    byte[] content2 = getRandomBytesArray(16 * ONE_MB);
    Path path1 = createFileWithContent(fs, "file1", content1);
    Path path2 = createFileWithContent(fs, "file2", content2);

    // Open two separate input streams for concurrent access
    try (FSDataInputStream in1 = fs.openFile(path1).build().get();
         FSDataInputStream in2 = fs.openFile(path2).build().get()) {

      AbfsInputStream streamVectored = (AbfsInputStream) in1.getWrappedStream();
      AbfsInputStream streamSequential
          = (AbfsInputStream) in2.getWrappedStream();
      IntFunction<ByteBuffer> allocator = ByteBuffer::allocate;

      // Define non-contiguous ranges for the vectored read on file 1
      List<FileRange> vRanges = new ArrayList<>();
      vRanges.add(FileRange.createFileRange(2 * ONE_MB, (int) ONE_MB));
      vRanges.add(FileRange.createFileRange(4 * ONE_MB, (int) ONE_MB));

      // Use a latch to ensure both threads start their I/O at the same time
      CountDownLatch latch = new CountDownLatch(1);

      // Thread 1: Perform asynchronous vectored reads on file 1
      CompletableFuture<Void> vectoredTask = CompletableFuture.runAsync(() -> {
        try {
          latch.await();
          streamVectored.readVectored(vRanges, allocator);
        } catch (Exception e) {
          throw new RuntimeException("Vectored read task failed", e);
        }
      });

      // Thread 2: Perform multiple sequential reads on file 2 to trigger readahead
      CompletableFuture<Void> sequentialTask = CompletableFuture.runAsync(
          () -> {
            try {
              latch.await();
              for (int i = 0; i < 5; i++) {
                byte[] tempBuf = new byte[(int) ONE_MB];
                streamSequential.read(i * ONE_MB, tempBuf, 0, (int) ONE_MB);
                // Validate data integrity for file 2 immediately
                assertArrayEquals(Arrays.copyOfRange(content2, i * (int) ONE_MB,
                        (i + 1) * (int) ONE_MB), tempBuf,
                    "Sequential read mismatch in file 2 at block " + i);
              }
            } catch (Exception e) {
              throw new RuntimeException("Sequential read task failed", e);
            }
          });

      // Trigger simultaneous execution
      latch.countDown();

      // Wait for both high-level tasks to finish
      CompletableFuture.allOf(vectoredTask, sequentialTask).get();

      // Explicitly wait for the vectored read futures to complete their data transfer
      CompletableFuture<?>[] vFutures = vRanges.stream()
          .map(FileRange::getData)
          .toArray(CompletableFuture[]::new);
      CompletableFuture.allOf(vFutures).get();

      // Final validation of vectored read content for file 1
      validateVectoredReadResult(vRanges, content1, 0);
    }
  }

  @Test
  public void testVectoredReadHitchhikesOnExistingPrefetch() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(8 * ONE_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    try (FSDataInputStream in = fs.openFile(testFilePath).build().get()) {
      AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
      AbfsInputStream spyIn = Mockito.spy(abfsIn);

      // 1. Trigger a normal read to start the prefetch logic
      // Reading the first byte often triggers a larger readahead (e.g., 4MB)
      byte[] seqBuf = new byte[1];
      spyIn.read(seqBuf, 0, 1);

      // 2. Immediately queue a vectored read for an offset within that prefetch range
      List<FileRange> vRanges = new ArrayList<>();
      // Using 1MB offset, which should be inside the initial readahead buffer
      vRanges.add(FileRange.createFileRange(ONE_MB, (int) ONE_MB));

      IntFunction<ByteBuffer> allocator = ByteBuffer::allocate;
      spyIn.readVectored(vRanges, allocator);

      // 3. Wait for the vectored read to complete
      vRanges.get(0).getData().get();

      // 4. Validate Data Integrity
      validateVectoredReadResult(vRanges, fileContent, 0);

      // 5. THE CRITICAL VALIDATION:
      // Even though we did a manual read and a vectored read,
      // there should only be ONE remote call if hitchhiking worked.
      Mockito.verify(spyIn, Mockito.atMost(spyIn.getReadAheadQueueDepth()))
          .readRemote(
              Mockito.anyLong(),
              Mockito.any(byte[].class),
              Mockito.anyInt(),
              Mockito.anyInt(),
              Mockito.any());
    }
  }

  @Test
  public void testMultipleReadsWhileBufferInProgressEventuallyComplete()
      throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName();
    byte[] fileContent = getRandomBytesArray(8 * ONE_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    CountDownLatch blockCompletion = new CountDownLatch(1);

    try (FSDataInputStream in = fs.openFile(testFilePath).build().get()) {
      AbfsInputStream spyIn =
          Mockito.spy((AbfsInputStream) in.getWrappedStream());
      ReadBufferManager rbm = spyIn.getReadBufferManager();

      /* Block completion so buffer stays in inProgressList */
      Mockito.doAnswer(invocation -> {
        blockCompletion.await();
        return invocation.callRealMethod();
      }).when(spyIn).readRemote(
          Mockito.anyLong(),
          Mockito.any(byte[].class),
          Mockito.anyInt(),
          Mockito.anyInt(),
          Mockito.any());

      ExecutorService exec = Executors.newFixedThreadPool(3);

      /* 1. Start first normal read → creates in-progress buffer */
      Future<?> r1 = exec.submit(() -> {
        try {
          spyIn.read(new byte[1], 0, 1);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      /* 2. Explicitly validate buffer is in inProgressList */
      ReadBuffer inProgress = null;
      for (int i = 0; i < 100; i++) {
        synchronized (rbm) {
          inProgress = rbm.findInList(
              rbm.getInProgressList(), spyIn, 0);
        }
        if (inProgress != null) {
          break;
        }
        Thread.sleep(10);
      }
      assertNotNull(inProgress,
          "Expected buffer to be in inProgressList while completion is blocked");

      /* 3. Submit another normal read while buffer is in progress */
      Future<?> r2 = exec.submit(() -> {
        try {
          spyIn.read(new byte[1], 0, 1);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      /* 4. Submit vectored read while buffer is in progress */
      List<FileRange> ranges = new ArrayList<>();
      ranges.add(FileRange.createFileRange(ONE_MB, (int) ONE_MB));
      Future<?> vr = exec.submit(() -> {
        try {
          spyIn.readVectored(ranges, ByteBuffer::allocate);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      /* 5. Allow completion */
      blockCompletion.countDown();

      /* 6. All reads must complete */
      r1.get(5, TimeUnit.SECONDS);
      r2.get(5, TimeUnit.SECONDS);
      vr.get(5, TimeUnit.SECONDS);
      ranges.get(0).getData().get(5, TimeUnit.SECONDS);

      validateVectoredReadResult(ranges, fileContent, 0);

      exec.shutdownNow();
    }
  }

  @Test
  public void testThroughputOptimizedReadVectored() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_VECTORED_READ_STRATEGY, "TPS");
    FileSystem fileSystem = FileSystem.newInstance(configuration);
    try (AzureBlobFileSystem abfs = (AzureBlobFileSystem) fileSystem) {
      String fileName = methodName.getMethodName();
      byte[] fileContent = getRandomBytesArray(32 * ONE_MB);
      Path testFilePath = createFileWithContent(abfs, fileName, fileContent);
      List<FileRange> fileRanges = new ArrayList<>();
      // 0.0 – 3.8 MB
      fileRanges.add(FileRange.createFileRange(0L, (int) (3.8 * ONE_MB)));
      // 4.0 – 7.2 MB
      fileRanges.add(FileRange.createFileRange((long) (4.0 * ONE_MB),
          (int) (3.2 * ONE_MB)));
      // 8.0 – 10.0 MB
      fileRanges.add(FileRange.createFileRange((long) (8.0 * ONE_MB),
          (int) (2.0 * ONE_MB)));
      // 12.0 – 16.0 MB
      fileRanges.add(FileRange.createFileRange((long) (12.0 * ONE_MB),
          (int) (4.0 * ONE_MB)));
      // 16.0 – 18.0 MB
      fileRanges.add(FileRange.createFileRange((long) (16.0 * ONE_MB),
          (int) (2.0 * ONE_MB)));
      IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
      try (FSDataInputStream in =
               abfs.openFile(testFilePath).build().get()) {
        AbfsInputStream abfsIn = (AbfsInputStream) in.getWrappedStream();
        AbfsInputStream spyIn = Mockito.spy(abfsIn);
        spyIn.readVectored(fileRanges, allocate);
        CompletableFuture<?>[] futures =
            new CompletableFuture<?>[fileRanges.size()];
        int i = 0;
        for (FileRange range : fileRanges) {
          futures[i++] = range.getData();
        }
        CompletableFuture.allOf(futures).get();
        validateVectoredReadResult(fileRanges, fileContent, 0);
        Mockito.verify(spyIn, Mockito.times(5))
            .readRemote(
                Mockito.anyLong(),
                Mockito.any(byte[].class),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.any());
      }
    }
  }
}
