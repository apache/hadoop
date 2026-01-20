package org.apache.hadoop.fs.azurebfs.services;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

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
    CompletableFuture<FSDataInputStream> builder = fs.openFile(testFilePath).build();

    try (FSDataInputStream in = builder.get()) {
      in.readVectored(rangeList, allocate);
      byte[] readFullRes = new byte[(int)fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(rangeList, readFullRes, 0);
    }
  }

  @Test
  public void testVectoredReadDisjointRanges() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName() + 1;
    byte[] fileContent = getRandomBytesArray(ONE_MB);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(FileRange.createFileRange(100, 10000));
    fileRanges.add(FileRange.createFileRange(15000, 12000));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    try (FSDataInputStream in =
             fs.openFile(testFilePath).build().get()) {

      in.readVectored(fileRanges, allocate);
      CompletableFuture<?>[] futures =
          new CompletableFuture<?>[fileRanges.size()];
      int i = 0;
      for (FileRange range : fileRanges) {
        futures[i++] = range.getData();
      }
      CompletableFuture.allOf(futures).get();
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
    CompletableFuture<FSDataInputStream> builder = fs.openFile(testFilePath).build();

    try (FSDataInputStream in = builder.get()) {
      in.readVectored(rangeList, allocate);
      byte[] readFullRes = new byte[(int)fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(rangeList, readFullRes, 0);
    }
  }

  @Test
  public void testMultipleRangesWithVectoredRead() throws Throwable {
    int fileSize = ONE_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName() + 1;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> rangeList = new ArrayList<>();
    rangeList.add(FileRange.createFileRange(100, 10000));
    rangeList.add(FileRange.createFileRange(15000, 27000));
    rangeList.add(FileRange.createFileRange(47500, 27000));

    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    CompletableFuture<FSDataInputStream> builder = fs.openFile(testFilePath).build();

    try (FSDataInputStream in = builder.get()) {
      in.readVectored(rangeList, allocate);
      byte[] readFullRes = new byte[(int)fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(rangeList, readFullRes, 0);
    }
  }

  @Test
  public void testMergedRangesWithVectoredRead() throws Throwable {
    int fileSize = ONE_MB;
    final AzureBlobFileSystem fs = getFileSystem();
    String fileName = methodName.getMethodName() + 1;
    byte[] fileContent = getRandomBytesArray(fileSize);
    Path testFilePath = createFileWithContent(fs, fileName, fileContent);

    List<FileRange> rangeList = new ArrayList<>();
    rangeList.add(FileRange.createFileRange(100, 10000));
    rangeList.add(FileRange.createFileRange(12000, 27000));
    IntFunction<ByteBuffer> allocate = ByteBuffer::allocate;
    CompletableFuture<FSDataInputStream> builder = fs.openFile(testFilePath).build();

    try (FSDataInputStream in = builder.get()) {
      in.readVectored(rangeList, allocate);
      byte[] readFullRes = new byte[(int)fileSize];
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
      long timeMilli1 = System.currentTimeMillis();
      in.readVectored(rangeList, allocate);
      byte[] readFullRes = new byte[(int)fileSize];
      in.readFully(0, readFullRes);
      // Comparing vectored read results with read fully.
      validateVectoredReadResult(rangeList, readFullRes, 0);
      long timeMilli2 = System.currentTimeMillis();
      System.out.println("Time taken for the code to execute: " + (timeMilli2 - timeMilli1) + " milliseconds");
    }
  }
}
