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

import java.util.Arrays;
import java.util.Collection;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.NetworkBinding;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.SSL_CHANNEL_MODE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Make sure that wildfly is not on this classpath and that we can still
 * create connections.
 */
@RunWith(Parameterized.class)
public class TestOpenSSLClasspaths extends AbstractHadoopTestBase {

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {DelegatingSSLSocketFactory.SSLChannelMode.OpenSSL, false},
        {DelegatingSSLSocketFactory.SSLChannelMode.Default, true},
        {DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE, true},
        {DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE_with_GCM, true},
    });
  }

  private final DelegatingSSLSocketFactory.SSLChannelMode mode;
  private final boolean expectSuccess;

  public TestOpenSSLClasspaths(
      final DelegatingSSLSocketFactory.SSLChannelMode mode,
      final boolean expectSuccess) {
    this.mode = mode;
    this.expectSuccess = expectSuccess;
  }

  @Test
  public void testWildflyOffCP() throws Throwable {
    // make sure wildfly is off the CP, and yet
    // all our tests work
    ClassLoader loader = this.getClass().getClassLoader();
    intercept(ClassNotFoundException.class, () ->
        loader.loadClass("org.wildfly.openssl.OpenSSLProvider"));
  }

  @Test
  public void testOpenSSLBindingDowngrades() throws Throwable {
    Configuration conf = new Configuration(false);
    conf.set(SSL_CHANNEL_MODE, mode.name());
    ClientConfiguration awsConf = new ClientConfiguration();
    awsConf.setProtocol(Protocol.HTTPS);
    Assertions.assertThat(
        NetworkBinding.bindSSLChannelMode(conf, awsConf))
        .describedAs("SSL binding for channel mode %s", mode)
        .isEqualTo(expectSuccess);
  }
}
