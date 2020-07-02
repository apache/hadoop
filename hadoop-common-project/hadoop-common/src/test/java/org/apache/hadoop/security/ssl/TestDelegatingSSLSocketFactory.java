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

package org.apache.hadoop.security.ssl;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import org.apache.hadoop.util.NativeCodeLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for {@link DelegatingSSLSocketFactory}.
 */
public class TestDelegatingSSLSocketFactory {

  @Test
  public void testOpenSSL() throws IOException {
    assumeTrue("Unable to load native libraries",
            NativeCodeLoader.isNativeCodeLoaded());
    assumeTrue("Build was not compiled with support for OpenSSL",
            NativeCodeLoader.buildSupportsOpenssl());
    DelegatingSSLSocketFactory.initializeDefaultFactory(
            DelegatingSSLSocketFactory.SSLChannelMode.OpenSSL);
    assertThat(DelegatingSSLSocketFactory.getDefaultFactory()
            .getProviderName()).contains("openssl");
  }

  @Test
  public void testJSEENoGCMJava8() throws IOException {
    assumeTrue("Not running on Java 8",
            System.getProperty("java.version").startsWith("1.8"));
    DelegatingSSLSocketFactory.initializeDefaultFactory(
            DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE);
    assertThat(Arrays.stream(DelegatingSSLSocketFactory.getDefaultFactory()
            .getSupportedCipherSuites())).noneMatch("GCM"::contains);
  }
}
