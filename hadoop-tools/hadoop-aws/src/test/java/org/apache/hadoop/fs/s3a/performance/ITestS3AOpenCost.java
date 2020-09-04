/*
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

package org.apache.hadoop.fs.s3a.performance;


import java.io.EOFException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.readStream;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile;
import static org.apache.hadoop.fs.OpenFileOptions.FS_OPTION_OPENFILE_FADVISE;
import static org.apache.hadoop.fs.OpenFileOptions.FS_OPTION_OPENFILE_FADVISE_SEQUENTIAL;
import static org.apache.hadoop.fs.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_CLOSE_BYTES_READ;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_OPENED;
import static org.apache.hadoop.fs.s3a.Statistic.STREAM_SEEK_BYTES_SKIPPED;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.NO_IO;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Cost of openFile().
 */
@RunWith(Parameterized.class)
public class ITestS3AOpenCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AOpenCost.class);

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"raw-keep-markers", false, true, false},
    });
  }

  public ITestS3AOpenCost(final String name,
      final boolean s3guard,
      final boolean keepMarkers,
      final boolean authoritative) {
    super(s3guard, keepMarkers, authoritative);
  }

  /**
   * Test when openFile() performs GET requests when file status
   * and length options are passed down.
   * Note that the input streams only update the FS statistics
   * in close(), so metrics cannot be verified until all operations
   * on a stream are complete.
   * This is slightly less than ideal.
   */
  @Test
  public void testOpenFileCost() throws Throwable {
    describe("Test cost of openFile with/without status; raw only");
    S3AFileSystem fs = getFileSystem();
    Path testFile = path("testOpenFileCost");

    writeTextFile(fs, testFile, "openfile", true);
    FileStatus st = fs.getFileStatus(testFile);

    // now read that file back in using the openFile call.
    // with a new FileStatus and a different path.
    // this verifies that any FileStatus class/subclass is used
    // as a source of the file length.
    long len = st.getLen();
    FileStatus st2 = new FileStatus(
        len, false,
        st.getReplication(),
        st.getBlockSize(),
        st.getModificationTime(),
        st.getAccessTime(),
        st.getPermission(),
        st.getOwner(),
        st.getGroup(),
        new Path("gopher:///localhost/" + testFile.getName()));

    // no IO
    FSDataInputStream in = verifyMetrics(() ->
            fs.openFile(testFile)
                .withFileStatus(st)
                .build()
                .get(),
        always(NO_IO),
        with(STREAM_OPENED, 0));

    long readLen = verifyMetrics(() ->
            readStream(in),
        always(NO_IO),
        with(STREAM_OPENED, 1));


    assertEquals("bytes read from file", len, readLen);

    // do a second read with the length declared as short.
    // we now expect the bytes read to be shorter.
    S3ATestUtils.MetricDiff bytesDiscarded =
        new S3ATestUtils.MetricDiff(fs, STREAM_CLOSE_BYTES_READ);
    int offset = 2;
    long shortLen = len - offset;
    CompletableFuture<FSDataInputStream> f2 = fs.openFile(testFile)
        .must(FS_OPTION_OPENFILE_FADVISE, FS_OPTION_OPENFILE_FADVISE_SEQUENTIAL)
        .opt(FS_OPTION_OPENFILE_LENGTH, shortLen)
        .build();
    FSDataInputStream in2 = verifyMetrics(() ->
            fs.openFile(testFile)
                .must(FS_OPTION_OPENFILE_FADVISE,
                    FS_OPTION_OPENFILE_FADVISE_SEQUENTIAL)
                .opt(FS_OPTION_OPENFILE_LENGTH, shortLen)
                .build()
                .get(),
        always(NO_IO),
        with(STREAM_OPENED, 0));

    long r2 = verifyMetrics(() ->
            readStream(in2),
        always(NO_IO),
        with(STREAM_OPENED, 1),
        with(STREAM_CLOSE_BYTES_READ, 0),
        with(STREAM_SEEK_BYTES_SKIPPED, 0));

    assertEquals("bytes read from file", shortLen, r2);
    // the read has been ranged
    bytesDiscarded.assertDiffEquals(0);

    long longLen = len + 10;
    FSDataInputStream in3 = verifyMetrics(() ->
            fs.openFile(testFile)
                .must(FS_OPTION_OPENFILE_FADVISE,
                    FS_OPTION_OPENFILE_FADVISE_SEQUENTIAL)
                .must(FS_OPTION_OPENFILE_LENGTH, longLen)
                .build()
                .get(),
        always(NO_IO));

    // shows that irrespective of the declared length, you can read past it.
    verifyMetrics(() -> {
          byte[] out = new byte[(int) longLen];
          intercept(EOFException.class,
              () -> in3.readFully(0, out));
          in3.seek(longLen - 1);
          assertEquals("read past real EOF on " + in3,
              -1, in3.read());
          in3.close();
          return null;
        },
        // two GET calls were made, one for readFully,
        // the second on the read() past the EOF
        // the operation has got as far as S3
        with(STREAM_OPENED, 2));

  }
}
