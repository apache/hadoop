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

import java.io.IOException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.Constants.SSL_CHANNEL_MODE;
import static org.apache.hadoop.fs.s3a.impl.NetworkBinding.bindSSLChannelMode;
import static org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory.SSLChannelMode.Default;
import static org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE;
import static org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE_with_GCM;
import static org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory.SSLChannelMode.OpenSSL;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Make sure that wildfly is not on this classpath and that we can still
 * create connections in the default option, but that openssl fails.
 * This test suite is designed to work whether or not wildfly JAR is on
 * the classpath, and when openssl native libraries are/are not
 * on the path.
 * Some of the tests are skipped in a maven build because wildfly
 * is always on the classpath -but they are retained as in-IDE
 * runs may be different, and if wildfly is removed from
 * the compile or test CP then different test cases will execute.
 */
public class TestWildflyAndOpenSSLBinding extends AbstractHadoopTestBase {

  /** Was wildfly found. */
  private boolean hasWildfly;

  @Before
  public void setup() throws Exception {
    // determine whether or not wildfly is on the classpath
    ClassLoader loader = this.getClass().getClassLoader();
    try {
      loader.loadClass("org.wildfly.openssl.OpenSSLProvider");
      hasWildfly = true;
    } catch (ClassNotFoundException e) {
      hasWildfly = false;
    }
  }


  @Test
  public void testUnknownMode() throws Throwable {
    DelegatingSSLSocketFactory.resetDefaultFactory();
    Configuration conf = new Configuration(false);
    conf.set(SSL_CHANNEL_MODE, "no-such-mode ");
    intercept(IllegalArgumentException.class, () ->
        bindSSLChannelMode(conf, new ClientConfiguration()));
  }

  @Test
  public void testOpenSSLNoWildfly() throws Throwable {
    assumeThat(hasWildfly).isFalse();
    intercept(NoClassDefFoundError.class, "wildfly", () ->
      bindSocketFactory(OpenSSL));
  }

  /**
   * If there is no WF on the CP, then we always downgrade
   * to default.
   */
  @Test
  public void testDefaultDowngradesNoWildfly() throws Throwable {
    assumeThat(hasWildfly).isFalse();
    expectBound(Default, Default_JSSE);
  }

  /**
   * Wildfly is on the CP; if openssl native is on the
   * path then openssl will load, otherwise JSSE.
   */
  @Test
  public void testWildflyOpenSSL() throws Throwable {
    assumeThat(hasWildfly).isTrue();
    assertThat(bindSocketFactory(Default))
        .describedAs("Sockets from mode " + Default)
        .isIn(OpenSSL, Default_JSSE);
  }

  @Test
  public void testJSSE() throws Throwable {
    expectBound(Default_JSSE, Default_JSSE);
  }

  @Test
  public void testGCM() throws Throwable {
    expectBound(Default_JSSE_with_GCM, Default_JSSE_with_GCM);
  }

  /**
   * Bind to a socket mode and verify that the result matches
   * that expected -which does not have to be the one requested.
   * @param channelMode mode to use
   * @param finalMode mode to test for
   */
  private void expectBound(
      DelegatingSSLSocketFactory.SSLChannelMode channelMode,
      DelegatingSSLSocketFactory.SSLChannelMode finalMode)
      throws Throwable {
    assertThat(bindSocketFactory(channelMode))
        .describedAs("Channel mode of socket factory created with mode %s",
            channelMode)
        .isEqualTo(finalMode);
  }

  /**
   * Bind the socket factory to a given channel mode.
   * @param channelMode mode to use
   * @return the actual channel mode.
   */
  private DelegatingSSLSocketFactory.SSLChannelMode bindSocketFactory(
      final DelegatingSSLSocketFactory.SSLChannelMode channelMode)
      throws IOException {
    DelegatingSSLSocketFactory.resetDefaultFactory();
    Configuration conf = new Configuration(false);
    conf.set(SSL_CHANNEL_MODE, channelMode.name());
    ClientConfiguration awsConf = new ClientConfiguration();
    awsConf.setProtocol(Protocol.HTTPS);
    bindSSLChannelMode(conf, awsConf);
    return DelegatingSSLSocketFactory.getDefaultFactory().getChannelMode();
  }

}
