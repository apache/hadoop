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

package org.apache.hadoop.fs.s3a;

import static org.mockito.Mockito.*;

import java.net.URI;
import java.util.ArrayList;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * An {@link S3ClientFactory} that returns Mockito mocks of the {@link S3Client}
 * interface suitable for unit testing.
 */
public class MockS3ClientFactory implements S3ClientFactory {


  @Override
  public S3Client createS3Client(URI uri, final S3ClientCreationParameters parameters) {
    S3Client s3 = mock(S3Client.class);
    // this listing is used in startup if purging is enabled, so
    // return a stub value
    ListMultipartUploadsResponse noUploads = ListMultipartUploadsResponse.builder()
        .uploads(new ArrayList<>(0))
        .isTruncated(false)
        .build();
    when(s3.listMultipartUploads((ListMultipartUploadsRequest) any())).thenReturn(noUploads);
    when(s3.getBucketLocation((GetBucketLocationRequest) any())).thenReturn(
        GetBucketLocationResponse.builder().locationConstraint(Region.US_WEST_2.toString())
            .build());
    return s3;
  }

  @Override
  public S3AsyncClient createS3AsyncClient(URI uri, final S3ClientCreationParameters parameters) {
    S3AsyncClient s3 = mock(S3AsyncClient.class);
    return s3;
  }

  @Override
  public S3TransferManager createS3TransferManager(S3AsyncClient s3AsyncClient) {
    S3TransferManager tm = mock(S3TransferManager.class);
    return tm;
  }
}
