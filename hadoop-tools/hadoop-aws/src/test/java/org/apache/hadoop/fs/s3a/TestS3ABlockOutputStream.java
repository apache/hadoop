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

import org.apache.hadoop.fs.s3a.commit.PutTracker;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for {@link S3ABlockOutputStream}.
 */
public class TestS3ABlockOutputStream extends AbstractS3AMockTest {

  private S3ABlockOutputStream stream;

  @Before
  public void setUp() throws Exception {
    ExecutorService executorService = mock(ExecutorService.class);
    Progressable progressable = mock(Progressable.class);
    S3ADataBlocks.BlockFactory blockFactory =
        mock(S3ADataBlocks.BlockFactory.class);
    long blockSize = Constants.DEFAULT_MULTIPART_SIZE;
    S3AInstrumentation.OutputStreamStatistics statistics = null;
    WriteOperationHelper oHelper = mock(WriteOperationHelper.class);
    PutTracker putTracker = mock(PutTracker.class);
    stream = spy(new S3ABlockOutputStream(fs, "", executorService,
      progressable, blockSize, blockFactory, statistics, oHelper,
      putTracker));
  }

  @Test
  public void testFlushNoOpWhenStreamClosed() throws Exception {
    doThrow(new IOException()).when(stream).checkOpen();

    try {
      stream.flush();
    } catch (Exception e){
      fail("Should not have any exception.");
    }
  }
}
