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
import org.apache.hadoop.fs.s3a.impl.InstantiationIOException;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.junit.Assert;
import org.junit.Test;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESS_GRANTS_ENABLED;
import static org.junit.Assert.assertEquals;


/**
 * Test S3 Access Grants configurations.
 */
public class TestS3AccessGrantConfiguration extends AbstractHadoopTestBase {

  @Test
  public void testS3AccessGrantsEnabled() {
    applyVerifyS3AGPlugin(S3Client.builder(), false, true);
  }

  @Test
  public void testS3AccessGrantsEnabledAsync() {
    applyVerifyS3AGPlugin(S3AsyncClient.builder(), false, true);
  }

  @Test
  public void testS3AccessGrantsDisabled() {
    applyVerifyS3AGPlugin(S3Client.builder(), false, false);
  }

  @Test
  public void testS3AccessGrantsDisabledByDefault() {
    applyVerifyS3AGPlugin(S3Client.builder(), true, false);
  }

  @Test
  public void testS3AccessGrantsDisabledAsync() {
    applyVerifyS3AGPlugin(S3AsyncClient.builder(), false, false);
  }

  @Test
  public void testS3AccessGrantsDisabledByDefaultAsync() {
    applyVerifyS3AGPlugin(S3AsyncClient.builder(), true, false);
  }

  private Configuration createConfig(boolean isDefault, boolean s3agEnabled) {
    Configuration conf = new Configuration();
    if (!isDefault){
      conf.setBoolean(AWS_S3_ACCESS_GRANTS_ENABLED, s3agEnabled);
    }
    return conf;
  }

  private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> void
  applyVerifyS3AGPlugin(BuilderT builder, boolean isDefault, boolean enabled) {
    DefaultS3ClientFactory.applyS3AccessGrantsConfigurations(builder, createConfig(isDefault, enabled));
    if (enabled){
      assertEquals(builder.plugins().size(), 1);
      assertEquals(builder.plugins().get(0).getClass().getName(),
          "software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin");
    }
    else {
      assertEquals(builder.plugins().size(), 0);
    }
  }
}