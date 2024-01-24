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
import org.junit.Test;

import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESS_GRANTS_ENABLED;
import static org.junit.Assert.assertEquals;


/**
 * Test S3 Access Grants configurations
 */
public class TestS3AccessGrantConfiguration {

  @Test
  public void testS3AccessGrantsEnabled() {
    Configuration conf = new Configuration();
    conf.set(AWS_S3_ACCESS_GRANTS_ENABLED, "true");
    S3ClientBuilder builder = S3Client.builder();
    DefaultS3ClientFactory.applyS3AccessGrantsConfigurations(builder, conf);
    verifyS3AGPluginEnabled(builder);
  }

  @Test
  public void testS3AccessGrantsEnabledAsync() {
    Configuration conf = new Configuration();
    conf.set(AWS_S3_ACCESS_GRANTS_ENABLED, "true");
    S3AsyncClientBuilder builder = S3AsyncClient.builder();
    DefaultS3ClientFactory.applyS3AccessGrantsConfigurations(builder, conf);
    verifyS3AGPluginEnabled(builder);
  }

  @Test
  public void testS3AccessGrantsDisabled() {
    Configuration conf = new Configuration();
    conf.set(AWS_S3_ACCESS_GRANTS_ENABLED, "false");
    S3ClientBuilder builder = S3Client.builder();
    DefaultS3ClientFactory.applyS3AccessGrantsConfigurations(builder, conf);
    verifyS3AGPluginDisabled(builder);
  }

  @Test
  public void testS3AccessGrantsDisabledByDefault() {
    Configuration conf = new Configuration();
    S3ClientBuilder builder = S3Client.builder();
    DefaultS3ClientFactory.applyS3AccessGrantsConfigurations(builder, conf);
    verifyS3AGPluginDisabled(builder);
  }

  @Test
  public void testS3AccessGrantsDisabledAsync() {
    Configuration conf = new Configuration();
    conf.set(AWS_S3_ACCESS_GRANTS_ENABLED, "false");
    S3AsyncClientBuilder builder = S3AsyncClient.builder();
    DefaultS3ClientFactory.applyS3AccessGrantsConfigurations(builder, conf);
    verifyS3AGPluginDisabled(builder);
  }

  @Test
  public void testS3AccessGrantsDisabledByDefaultAsync() {
    Configuration conf = new Configuration();
    S3AsyncClientBuilder builder = S3AsyncClient.builder();
    DefaultS3ClientFactory.applyS3AccessGrantsConfigurations(builder, conf);
    verifyS3AGPluginDisabled(builder);
  }

  private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> void verifyS3AGPluginEnabled(BuilderT builder) {
    assertEquals(builder.plugins().size(), 1);
    assertEquals(builder.plugins().get(0).getClass().getName(),
        "software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin");
  }

  private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> void verifyS3AGPluginDisabled(BuilderT builder) {
    assertEquals(builder.plugins().size(),0);
  }
}