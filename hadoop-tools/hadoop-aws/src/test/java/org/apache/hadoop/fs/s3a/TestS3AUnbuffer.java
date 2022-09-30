/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.junit.Test;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.time.Instant;

import static org.junit.Assert.assertEquals;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Uses mocks to check that the {@link ResponseInputStream<GetObjectResponse>} is
 * closed when {@link org.apache.hadoop.fs.CanUnbuffer#unbuffer} is called.
 * Unlike the other unbuffer tests, this specifically tests that the underlying
 * S3 object stream is closed.
 */
public class TestS3AUnbuffer extends AbstractS3AMockTest {

  @Test
  public void testUnbuffer() throws IOException {
    // Create mock ObjectMetadata for getFileStatus()
    Path path = new Path("/file");
    HeadObjectResponse objectMetadata = mock(HeadObjectResponse.class);
    when(objectMetadata.contentLength()).thenReturn(1L);
    when(objectMetadata.lastModified()).thenReturn(Instant.ofEpochMilli(2L));
    when(objectMetadata.eTag()).thenReturn("mock-etag");
    when(s3V2.headObject((HeadObjectRequest) any())).thenReturn(objectMetadata);

    // Create mock ResponseInputStream<GetObjectResponse> and GetObjectResponse for open()
    GetObjectResponse objectResponse = mock(GetObjectResponse.class);
    when(objectResponse.contentLength()).thenReturn(1L);
    when(objectResponse.lastModified()).thenReturn(Instant.ofEpochMilli(2L));
    when(objectResponse.eTag()).thenReturn("mock-etag");
    ResponseInputStream<GetObjectResponse> objectStream = mock(ResponseInputStream.class);
    when(objectStream.response()).thenReturn(objectResponse);
    when(objectStream.read()).thenReturn(-1);
    when(objectStream.read(any(byte[].class))).thenReturn(-1);
    when(objectStream.read(any(byte[].class), anyInt(), anyInt())).thenReturn(-1);
    when(s3V2.getObject((GetObjectRequest) any())).thenReturn(objectStream);

    // Call read and then unbuffer
    FSDataInputStream stream = fs.open(path);
    assertEquals(-1, stream.read(new byte[8])); // mocks read 0 bytes
    stream.unbuffer();

    // Verify that unbuffer closed the object stream
    verify(objectStream, atLeast(1)).close();
  }
}
