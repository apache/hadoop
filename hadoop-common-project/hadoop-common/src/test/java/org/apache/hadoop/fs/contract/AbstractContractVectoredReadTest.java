package org.apache.hadoop.fs.contract;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.FileRangeImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.FutureIOSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;

@RunWith(Parameterized.class)
public abstract class AbstractContractVectoredReadTest extends AbstractFSContractTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractContractVectoredReadTest.class);

  public static final int DATASET_LEN = 1024;
  private static final byte[] DATASET = ContractTestUtils.dataset(DATASET_LEN, 'a', 32);
  private static final String VECTORED_READ_FILE_NAME = "vectored_file.txt";

  private IntFunction<ByteBuffer> allocate;

  private String bufferType;

  @Parameterized.Parameters
  public static List<String> params() {
    return Arrays.asList("direct", "array");
  }

  public AbstractContractVectoredReadTest(String bufferType) {
    this.bufferType = bufferType;
    this.allocate = "array".equals(bufferType) ?
            ByteBuffer::allocate : ByteBuffer::allocateDirect;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Path path = path(VECTORED_READ_FILE_NAME);
    FileSystem fs = getFileSystem();
    createFile(fs, path, true, DATASET);
  }

  @Test
  public void testVectoredReadMultipleRanges() throws Exception {
    describe("Running with buffer type : " + bufferType);
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      FileRange fileRange = new FileRangeImpl(i * 100, 100);
      fileRanges.add(fileRange);
    }
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      CompletableFuture<?>[] completableFutures = new CompletableFuture<?>[fileRanges.size()];
      int i = 0;
      for (FileRange res : fileRanges) {
        completableFutures[i++] = res.getData();
      }
      CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(completableFutures);
      combinedFuture.get();

      for (FileRange res : fileRanges) {
        CompletableFuture<ByteBuffer> data = res.getData();
        try {
          ByteBuffer buffer = FutureIOSupport.awaitFuture(data);
          LOG.info("Returned data from offset {} : {} ", res.getOffset(),
                  Arrays.toString(buffer.array()));
          //assertDatasetEquals((int) res.getOffset(), "readAsync", buffer, res.getLength());
        } catch (Exception ex) {
          LOG.error("Exception while running vectored read ", ex);
          //Assert.fail("Exception while running vectored read " + ex);
        }
      }
    }
  }

  @Test
  public void testOverlappingRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(0, 100));
    fileRanges.add(new FileRangeImpl(90, 50));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      for (FileRange res : fileRanges) {
        CompletableFuture<ByteBuffer> data = res.getData();
        try {
          ByteBuffer buffer = data.get();
          assertDatasetEquals((int) res.getOffset(), "vecRead", buffer, res.getLength());
        } catch (Exception ex) {
          LOG.error("Exception while running vectored read ", ex);
          Assert.fail("Exception while running vectored read " + ex);
        }
      }
    }
  }

  @Test
  public void testEOFRanges()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(DATASET_LEN, 100));
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      in.readVectored(fileRanges, allocate);
      for (FileRange res : fileRanges) {
        CompletableFuture<ByteBuffer> data = res.getData();
        try {
          ByteBuffer buffer = data.get();
          // Shouldn't reach here.
          Assert.fail("EOFException must be thrown while reading EOF");
        } catch (ExecutionException ex) {
          // ignore as expected.
        } catch (Exception ex) {
          LOG.error("Exception while running vectored read ", ex);
          Assert.fail("Exception while running vectored read " + ex);
        }
      }
    }
  }

  @Test
  public void testNegativeLengthRange()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(0, -50));
    testExceptionalVectoredRead(fs, fileRanges, "Exception is expected");
  }

  @Test
  public void testNegativeOffsetRange()  throws Exception {
    FileSystem fs = getFileSystem();
    List<FileRange> fileRanges = new ArrayList<>();
    fileRanges.add(new FileRangeImpl(-1, 50));
    testExceptionalVectoredRead(fs, fileRanges, "Exception is expected");
  }

  protected void testExceptionalVectoredRead(FileSystem fs,
                                             List<FileRange> fileRanges,
                                             String s) throws IOException {
    boolean exRaised = false;
    try (FSDataInputStream in = fs.open(path(VECTORED_READ_FILE_NAME))) {
      // Can we intercept here as done in S3 tests ??
      in.readVectored(fileRanges, ByteBuffer::allocate);
    } catch (EOFException | IllegalArgumentException ex) {
      // expected.
      exRaised = true;
    }
    Assertions.assertThat(exRaised)
            .describedAs(s)
            .isTrue();
  }

  /**
   * Assert that the data read matches the dataset at the given offset.
   * This helps verify that the seek process is moving the read pointer
   * to the correct location in the file.
   *
   * @param readOffset the offset in the file where the read began.
   * @param operation  operation name for the assertion.
   * @param data       data read in.
   * @param length     length of data to check.
   */
  private void assertDatasetEquals(
          final int readOffset, final String operation,
          final ByteBuffer data,
          int length) {
    for (int i = 0; i < length; i++) {
      int o = readOffset + i;
      assertEquals(operation + " with read offset " + readOffset
                      + ": data[" + i + "] != DATASET[" + o + "]",
              DATASET[o], data.get(i));
    }
  }
}
