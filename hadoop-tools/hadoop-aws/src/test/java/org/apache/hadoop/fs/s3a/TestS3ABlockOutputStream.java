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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.audit.AuditTestSupport;
import org.apache.hadoop.fs.s3a.commit.PutTracker;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;
import org.apache.hadoop.fs.s3a.test.MinimalWriteOperationHelperCallbacks;
import org.apache.hadoop.fs.statistics.IOStatisticsContext;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;

import static org.apache.hadoop.fs.s3a.audit.AuditTestSupport.noopAuditor;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link S3ABlockOutputStream}.
 */
public class TestS3ABlockOutputStream extends AbstractS3AMockTest {

  private S3ABlockOutputStream stream;

  /**
   * Create an S3A Builder all mocked up from component pieces.
   * @return stream builder.
   */
  private S3ABlockOutputStream.BlockOutputStreamBuilder mockS3ABuilder() {
    ExecutorService executorService = mock(ExecutorService.class);
    Progressable progressable = mock(Progressable.class);
    S3ADataBlocks.BlockFactory blockFactory =
        mock(S3ADataBlocks.BlockFactory.class);
    long blockSize = Constants.DEFAULT_MULTIPART_SIZE;
    WriteOperationHelper oHelper = mock(WriteOperationHelper.class);
    PutTracker putTracker = mock(PutTracker.class);
    final S3ABlockOutputStream.BlockOutputStreamBuilder builder =
        S3ABlockOutputStream.builder()
            .withBlockFactory(blockFactory)
            .withBlockSize(blockSize)
            .withExecutorService(executorService)
            .withKey("")
            .withProgress(progressable)
            .withPutTracker(putTracker)
            .withWriteOperations(oHelper)
            .withPutOptions(PutObjectOptions.keepingDirs())
            .withIOStatisticsAggregator(
                IOStatisticsContext.getCurrentIOStatisticsContext()
                    .getAggregator());

    return builder;
  }

  @Before
  public void setUp() throws Exception {
    final S3ABlockOutputStream.BlockOutputStreamBuilder
        builder = mockS3ABuilder();
    stream = spy(new S3ABlockOutputStream(builder));
  }


  @Test
  public void testFlushNoOpWhenStreamClosed() throws Exception {
    doThrow(new StreamClosedException()).when(stream).checkOpen();

    stream.flush();
  }

  @Test
  public void testWriteOperationHelperPartLimits() throws Throwable {
    S3AFileSystem s3a = mock(S3AFileSystem.class);
    when(s3a.getBucket()).thenReturn("bucket");
    when(s3a.getRequestFactory())
        .thenReturn(MockS3AFileSystem.REQUEST_FACTORY);
    final Configuration conf = new Configuration();
    WriteOperationHelper woh = new WriteOperationHelper(s3a,
        conf,
        new EmptyS3AStatisticsContext(),
        noopAuditor(conf),
        AuditTestSupport.NOOP_SPAN,
        new MinimalWriteOperationHelperCallbacks(null)); // raises NPE if S3 client used
    // first one works
    String key = "destKey";
    woh.newUploadPartRequestBuilder(key,
        "uploadId", 1, false, 1024);
    // but ask past the limit and a PathIOE is raised
    intercept(PathIOException.class, key,
        () -> woh.newUploadPartRequestBuilder(key,
            "uploadId", 50000, true, 1024));
  }

  static class StreamClosedException extends ClosedIOException {

    StreamClosedException() {
      super("path", "message");
    }
  }

  @Test
  public void testStreamClosedAfterAbort() throws Exception {
    stream.abort();

    // This verification replaces testing various operations after calling
    // abort: after calling abort, stream is closed like calling close().
    intercept(ClosedIOException.class, () -> stream.checkOpen());

    // check that calling write() will call checkOpen() and throws exception
    doThrow(new StreamClosedException()).when(stream).checkOpen();

    intercept(StreamClosedException.class,
        () -> stream.write(new byte[] {'a', 'b', 'c'}));
  }

  @Test
  public void testCallingCloseAfterCallingAbort() throws Exception {
    stream.abort();

    // This shouldn't throw IOException like calling close() multiple times.
    // This will ensure abort() can be called with try-with-resource.
    stream.close();
  }


  /**
   * Unless configured to downgrade, the stream will raise exceptions on
   * Syncable API calls.
   */
  @Test
  public void testSyncableUnsupported() throws Exception {
    final S3ABlockOutputStream.BlockOutputStreamBuilder
        builder = mockS3ABuilder();
    builder.withDowngradeSyncableExceptions(false);
    stream = spy(new S3ABlockOutputStream(builder));
    intercept(UnsupportedOperationException.class, () -> stream.hflush());
    intercept(UnsupportedOperationException.class, () -> stream.hsync());
  }

  /**
   * When configured to downgrade, the stream downgrades on
   * Syncable API calls.
   */
  @Test
  public void testSyncableDowngrade() throws Exception {
    final S3ABlockOutputStream.BlockOutputStreamBuilder
        builder = mockS3ABuilder();
    builder.withDowngradeSyncableExceptions(true);
    stream = spy(new S3ABlockOutputStream(builder));

    stream.hflush();
    stream.hsync();
  }

}
