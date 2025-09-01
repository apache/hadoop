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

import org.apache.hadoop.fs.s3a.impl.streams.AnalyticsStreamFactory;
import org.apache.hadoop.fs.s3a.impl.streams.FactoryBindingParameters;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamFactory;
import org.apache.hadoop.service.CompositeService;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.fs.s3a.Constants.FS_S3A;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class TestAnalyticsInputStreamRetry extends TestS3AInputStreamRetry {

  protected S3AsyncClient s3Async;
  private FlakyS3StoreImpl s3Store;

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
    conf = createConfiguration();
    fs = new S3AFileSystem();
    URI uri = URI.create(FS_S3A + "://" + BUCKET);
    // unset S3CSE property from config to avoid pathIOE.
    conf.unset(Constants.S3_ENCRYPTION_ALGORITHM);
    conf.set(Constants.INPUT_STREAM_TYPE, Constants.INPUT_STREAM_TYPE_ANALYTICS);
    fs.initialize(uri, conf);
    s3Async = fs.getS3AInternals().getStore().getOrCreateAsyncClient();
    s3Store = new FlakyS3StoreImpl();

  }

  @Override
  protected ObjectInputStreamFactory getFactory() throws IOException {
    return s3Store.getFactory();
  }


  private class FlakyS3StoreImpl extends CompositeService {
    ObjectInputStreamFactory factory;

    public FlakyS3StoreImpl() throws Exception {
      super("FlakyS3Store");
      this.factory = new AnalyticsStreamFactory();
      addService(factory);
      super.serviceInit(conf);
      factory.bind(new FactoryBindingParameters(new FlakyCallbacks()));
    }

    public ObjectInputStreamFactory getFactory() {
      return factory;
    }

  }
  /**
   * Callbacks from {@link ObjectInputStreamFactory} instances.
   * Will throw connection exception twice on client.getObject() and succeed third time.
   */
  private class FlakyCallbacks implements ObjectInputStreamFactory.StreamFactoryCallbacks {
    AtomicInteger attempts = new AtomicInteger(0);
    AtomicInteger fail = new AtomicInteger(2);
    ConnectException exception = new ConnectException("Mock Connection Exception");
    @Override
    public S3AsyncClient getOrCreateAsyncClient(final boolean requireCRT) throws IOException {
      S3AsyncClientWrapper flakyClient = spy(new S3AsyncClientWrapper(s3Async));
      doAnswer(
          invocation ->
              CompletableFuture.supplyAsync(
                  () -> {
                    try {
                      InputStream flakyInputStream =
                          mockedInputStream(GetObjectResponse.builder().build(),
                              attempts.incrementAndGet() < fail.get(),
                              exception);

                      return new ResponseInputStream<>(
                          GetObjectResponse.builder().build(), flakyInputStream);
                    } catch (Throwable e) {
                      throw new RuntimeException(e);
                    }
                  }))
          .when(flakyClient)
          .getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
      return flakyClient;
    }

    @Override
    public void incrementFactoryStatistic(Statistic statistic) {
    }
  }
  /** Wrapper for S3 Async client, used to mock input stream
   * returned by the S3 Async client.
   */
  public static class S3AsyncClientWrapper implements S3AsyncClient {

    private final S3AsyncClient delegate;

    public S3AsyncClientWrapper(S3AsyncClient delegate) {
      this.delegate = delegate;
    }

    @Override
    public String serviceName() {
      return delegate.serviceName();
    }

    @Override
    public void close() {
      delegate.close();
    }

    @Override
    public <ReturnT> CompletableFuture<ReturnT> getObject(
        GetObjectRequest getObjectRequest,
        AsyncResponseTransformer<GetObjectResponse, ReturnT> asyncResponseTransformer) {
      return delegate.getObject(getObjectRequest, asyncResponseTransformer);
    }

    @Override
    public CompletableFuture<HeadObjectResponse> headObject(HeadObjectRequest headObjectRequest) {
      return delegate.headObject(headObjectRequest);
    }

    @Override
    public CompletableFuture<PutObjectResponse> putObject(
        PutObjectRequest putObjectRequest, AsyncRequestBody requestBody) {
      return delegate.putObject(putObjectRequest, requestBody);
    }

    @Override
    public CompletableFuture<CreateBucketResponse> createBucket(
        CreateBucketRequest createBucketRequest) {
      return delegate.createBucket(createBucketRequest);
    }

    @Override
    public S3ServiceClientConfiguration serviceClientConfiguration() {
      return delegate.serviceClientConfiguration();
    }
  }
}
