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

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.assertEquals;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Uses mocks to check that the {@link S3ObjectInputStream} is closed when
 * {@link org.apache.hadoop.fs.CanUnbuffer#unbuffer} is called. Unlike the
 * other unbuffer tests, this specifically tests that the underlying S3 object
 * stream is closed.
 */
public class TestS3AUnbuffer extends AbstractS3AMockTest {

  @Test
  public void testUnbuffer() throws IOException {
    // Create mock ObjectMetadata for getFileStatus()
    Path path = new Path("/file");
    ObjectMetadata meta = mock(ObjectMetadata.class);
    when(meta.getContentLength()).thenReturn(1L);
    when(meta.getLastModified()).thenReturn(new Date(2L));
    when(meta.getETag()).thenReturn("mock-etag");
    when(s3.getObjectMetadata(any())).thenReturn(meta);

    // Create mock S3ObjectInputStream and S3Object for open()
    S3ObjectInputStream objectStream = mock(S3ObjectInputStream.class);
    when(objectStream.read()).thenReturn(-1);

    S3Object s3Object = mock(S3Object.class);
    when(s3Object.getObjectContent()).thenReturn(objectStream);
    when(s3Object.getObjectMetadata()).thenReturn(meta);
    when(s3.getObject(any())).thenReturn(s3Object);

    // Call read and then unbuffer
    FSDataInputStream stream = fs.open(path);
    assertEquals(0, stream.read(new byte[8])); // mocks read 0 bytes
    stream.unbuffer();

    // Verify that unbuffer closed the object stream
    verify(objectStream, times(1)).close();
  }
}
