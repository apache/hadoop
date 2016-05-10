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
package org.apache.hadoop.fs.s3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import java.io.File;
import java.net.URI;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestS3Credentials {
  public static final Log LOG = LogFactory.getLog(TestS3Credentials.class);

  @Rule
  public final TestName test = new TestName();

  @Before
  public void announce() {
    LOG.info("Running test " + test.getMethodName());
  }

  private static final String EXAMPLE_ID = "AKASOMEACCESSKEY";
  private static final String EXAMPLE_KEY =
      "RGV0cm9pdCBSZ/WQgY2xl/YW5lZCB1cAEXAMPLE";

  @Test
  public void testInvalidHostnameWithUnderscores() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    try {
      s3Credentials.initialize(new URI("s3://a:b@c_d"), new Configuration());
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid hostname in URI s3://a:b@c_d", e.getMessage());
    }
  }

  @Test
  public void testPlaintextConfigPassword() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    Configuration conf = new Configuration();
    conf.set("fs.s3.awsAccessKeyId", EXAMPLE_ID);
    conf.set("fs.s3.awsSecretAccessKey", EXAMPLE_KEY);
    s3Credentials.initialize(new URI("s3://foobar"), conf);
    assertEquals("Could not retrieve proper access key", EXAMPLE_ID,
        s3Credentials.getAccessKey());
    assertEquals("Could not retrieve proper secret", EXAMPLE_KEY,
        s3Credentials.getSecretAccessKey());
  }

  @Test
  public void testPlaintextConfigPasswordWithWhitespace() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    Configuration conf = new Configuration();
    conf.set("fs.s3.awsAccessKeyId", "\r\n " + EXAMPLE_ID +
        " \r\n");
    conf.set("fs.s3.awsSecretAccessKey", "\r\n " + EXAMPLE_KEY +
        " \r\n");
    s3Credentials.initialize(new URI("s3://foobar"), conf);
    assertEquals("Could not retrieve proper access key", EXAMPLE_ID,
        s3Credentials.getAccessKey());
    assertEquals("Could not retrieve proper secret", EXAMPLE_KEY,
        s3Credentials.getSecretAccessKey());
  }

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testCredentialProvider() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = new Configuration();
    final File file = tempDir.newFile("test.jks");
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());

    // add our creds to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry("fs.s3.awsSecretAccessKey",
        EXAMPLE_KEY.toCharArray());
    provider.flush();

    // make sure S3Creds can retrieve things.
    S3Credentials s3Credentials = new S3Credentials();
    conf.set("fs.s3.awsAccessKeyId", EXAMPLE_ID);
    s3Credentials.initialize(new URI("s3://foobar"), conf);
    assertEquals("Could not retrieve proper access key", EXAMPLE_ID,
        s3Credentials.getAccessKey());
    assertEquals("Could not retrieve proper secret", EXAMPLE_KEY,
        s3Credentials.getSecretAccessKey());
  }

  @Test(expected=IllegalArgumentException.class)
  @Ignore
  public void noSecretShouldThrow() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    Configuration conf = new Configuration();
    conf.set("fs.s3.awsAccessKeyId", EXAMPLE_ID);
    s3Credentials.initialize(new URI("s3://foobar"), conf);
  }

  @Test(expected=IllegalArgumentException.class)
  @Ignore
  public void noAccessIdShouldThrow() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    Configuration conf = new Configuration();
    conf.set("fs.s3.awsSecretAccessKey", EXAMPLE_KEY);
    s3Credentials.initialize(new URI("s3://foobar"), conf);
  }
}
