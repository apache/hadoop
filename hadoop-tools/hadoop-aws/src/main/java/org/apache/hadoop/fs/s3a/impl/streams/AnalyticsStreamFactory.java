/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.impl.streams;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.VectoredIOContext;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.functional.LazyAutoCloseableReference;

import static org.apache.hadoop.fs.s3a.Constants.ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX;
import static org.apache.hadoop.fs.s3a.Statistic.ANALYTICS_STREAM_FACTORY_CLOSED;
import static org.apache.hadoop.fs.s3a.impl.streams.StreamIntegration.populateVectoredIOContext;

/**
 * A factory for {@link AnalyticsStream}. This class is instantiated during initialization of
 *  {@code S3AStore}, if fs.s3a.input.stream.type is set to Analytics.
 */
public class AnalyticsStreamFactory extends AbstractObjectInputStreamFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(AnalyticsStreamFactory.class);

  private S3SeekableInputStreamConfiguration seekableInputStreamConfiguration;
  private LazyAutoCloseableReference<S3SeekableInputStreamFactory>  s3SeekableInputStreamFactory;
  private boolean requireCrt;

  public AnalyticsStreamFactory() {
    super("AnalyticsStreamFactory");
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    ConnectorConfiguration configuration = new ConnectorConfiguration(conf,
                ANALYTICS_ACCELERATOR_CONFIGURATION_PREFIX);
    this.seekableInputStreamConfiguration =
                S3SeekableInputStreamConfiguration.fromConfiguration(configuration);
    this.requireCrt = false;
  }

  @Override
  public void bind(final FactoryBindingParameters factoryBindingParameters) throws IOException {
    super.bind(factoryBindingParameters);
    this.s3SeekableInputStreamFactory =
          new LazyAutoCloseableReference<>(createS3SeekableInputStreamFactory());
  }

  @Override
  public ObjectInputStream readObject(final ObjectReadParameters parameters) throws IOException {
    return new AnalyticsStream(
                parameters,
                getOrCreateS3SeekableInputStreamFactory());
  }

  @Override
  public InputStreamType streamType() {
    return InputStreamType.Analytics;
  }

  /**
   * Calculate Return StreamFactoryRequirements.
   * @return a positive thread count.
   */
  @Override
  public StreamFactoryRequirements factoryRequirements() {
    // fill in the vector context
    final VectoredIOContext vectorContext = populateVectoredIOContext(getConfig());
    // and then disable range merging.
    // this ensures that no reads are made for data which is then discarded...
    // so the prefetch and block read code doesn't ever do wasteful fetches.
    vectorContext.setMinSeekForVectoredReads(0);

    return new StreamFactoryRequirements(0,
            0, vectorContext,
            StreamFactoryRequirements.Requirements.ExpectUnauditedGetRequests);
  }

  @Override
  protected void serviceStop() throws Exception {
    try {
      s3SeekableInputStreamFactory.close();
    } catch (Exception ignored) {
      LOG.debug("Ignored exception while closing stream factory", ignored);
    }
    callbacks().incrementFactoryStatistic(ANALYTICS_STREAM_FACTORY_CLOSED);
    super.serviceStop();
  }

  private S3SeekableInputStreamFactory getOrCreateS3SeekableInputStreamFactory()
        throws IOException {
    return s3SeekableInputStreamFactory.eval();
  }

  private CallableRaisingIOE<S3SeekableInputStreamFactory> createS3SeekableInputStreamFactory() {
    return () -> new S3SeekableInputStreamFactory(
            new S3SdkObjectClient(callbacks().getOrCreateAsyncClient(requireCrt)),
            seekableInputStreamConfiguration);
  }

}
