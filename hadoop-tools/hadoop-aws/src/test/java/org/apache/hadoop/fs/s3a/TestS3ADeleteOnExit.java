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

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import org.mockito.ArgumentMatcher;


/**
 * deleteOnExit test for S3A.
 */
public class TestS3ADeleteOnExit extends AbstractS3AMockTest {

  static class TestS3AFileSystem extends S3AFileSystem {
    private int deleteOnDnExitCount;

    public int getDeleteOnDnExitCount() {
      return deleteOnDnExitCount;
    }

    @Override
    public boolean deleteOnExit(Path f) throws IOException {
      deleteOnDnExitCount++;
      return super.deleteOnExit(f);
    }

    // This is specifically designed for deleteOnExit processing.
    // In this specific case, deleteWithoutCloseCheck() will only be called in the path of
    // processDeleteOnExit.
    @Override
    protected boolean deleteWithoutCloseCheck(Path f, boolean recursive) throws IOException {
      boolean result = super.deleteWithoutCloseCheck(f, recursive);
      deleteOnDnExitCount--;
      return result;
    }
  }

  @Test
  public void testDeleteOnExit() throws Exception {
    Configuration conf = createConfiguration();
    TestS3AFileSystem testFs  = new TestS3AFileSystem();
    URI uri = URI.create(FS_S3A + "://" + BUCKET);
    // unset S3CSE property from config to avoid pathIOE.
    conf.unset(Constants.S3_ENCRYPTION_ALGORITHM);
    testFs.initialize(uri, conf);
    S3Client testS3 = testFs.getS3AInternals().getAmazonS3Client("mocking");

    Path path = new Path("/file");
    String key = path.toUri().getPath().substring(1);
    HeadObjectResponse objectMetadata =
        HeadObjectResponse.builder().contentLength(1L).lastModified(new Date(2L).toInstant())
            .build();
    when(testS3.headObject(argThat(correctGetMetadataRequest(BUCKET, key))))
            .thenReturn(objectMetadata);

    testFs.deleteOnExit(path);
    testFs.close();
    assertEquals(0, testFs.getDeleteOnDnExitCount());
  }

  private ArgumentMatcher<HeadObjectRequest> correctGetMetadataRequest(
          String bucket, String key) {
    return request -> request != null
            && request.bucket().equals(bucket)
            && request.key().equals(key);
  }
}
