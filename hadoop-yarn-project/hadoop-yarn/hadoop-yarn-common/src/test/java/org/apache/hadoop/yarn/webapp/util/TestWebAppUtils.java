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

package org.apache.hadoop.yarn.webapp.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.HttpServer2.Builder;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;
import org.junit.Assert;
import org.junit.Test;

public class TestWebAppUtils {

  @Test
  public void testGetPassword() throws Exception {
    Configuration conf = provisionCredentialsForSSL();

    // use WebAppUtils as would be used by loadSslConfiguration
    Assert.assertEquals("keypass",
        WebAppUtils.getPassword(conf, WebAppUtils.WEB_APP_KEY_PASSWORD_KEY));
    Assert.assertEquals("storepass",
        WebAppUtils.getPassword(conf, WebAppUtils.WEB_APP_KEYSTORE_PASSWORD_KEY));
    Assert.assertEquals("trustpass",
        WebAppUtils.getPassword(conf, WebAppUtils.WEB_APP_TRUSTSTORE_PASSWORD_KEY));

    // let's make sure that a password that doesn't exist returns null
    Assert.assertEquals(null, WebAppUtils.getPassword(conf,"invalid-alias"));
  }

  @Test
  public void testLoadSslConfiguration() throws Exception {
    Configuration conf = provisionCredentialsForSSL();
    TestBuilder builder = (TestBuilder) new TestBuilder();

    builder = (TestBuilder) WebAppUtils.loadSslConfiguration(
        builder, conf);

    String keypass = "keypass";
    String storepass = "storepass";
    String trustpass = "trustpass";    

    // make sure we get the right passwords in the builder
    assertEquals(keypass, ((TestBuilder)builder).keypass);
    assertEquals(storepass, ((TestBuilder)builder).keystorePassword);
    assertEquals(trustpass, ((TestBuilder)builder).truststorePassword);
  }

  protected Configuration provisionCredentialsForSSL() throws IOException,
      Exception {
    File testDir = new File(System.getProperty("test.build.data",
        "target/test-dir"));

    Configuration conf = new Configuration();
    final Path jksPath = new Path(testDir.toString(), "test.jks");
    final String ourUrl =
    JavaKeyStoreProvider.SCHEME_NAME + "://file" + jksPath.toUri();

    File file = new File(testDir, "test.jks");
    file.delete();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ourUrl);

    CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    char[] keypass = {'k', 'e', 'y', 'p', 'a', 's', 's'};
    char[] storepass = {'s', 't', 'o', 'r', 'e', 'p', 'a', 's', 's'};
    char[] trustpass = {'t', 'r', 'u', 's', 't', 'p', 'a', 's', 's'};

    // ensure that we get nulls when the key isn't there
    assertEquals(null, provider.getCredentialEntry(
        WebAppUtils.WEB_APP_KEY_PASSWORD_KEY));
    assertEquals(null, provider.getCredentialEntry(
        WebAppUtils.WEB_APP_KEYSTORE_PASSWORD_KEY));
    assertEquals(null, provider.getCredentialEntry(
        WebAppUtils.WEB_APP_TRUSTSTORE_PASSWORD_KEY));

    // create new aliases
    try {
      provider.createCredentialEntry(
          WebAppUtils.WEB_APP_KEY_PASSWORD_KEY, keypass);

      provider.createCredentialEntry(
          WebAppUtils.WEB_APP_KEYSTORE_PASSWORD_KEY, storepass);

      provider.createCredentialEntry(
          WebAppUtils.WEB_APP_TRUSTSTORE_PASSWORD_KEY, trustpass);

      // write out so that it can be found in checks
      provider.flush();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // make sure we get back the right key directly from api
    assertArrayEquals(keypass, provider.getCredentialEntry(
        WebAppUtils.WEB_APP_KEY_PASSWORD_KEY).getCredential());
    assertArrayEquals(storepass, provider.getCredentialEntry(
        WebAppUtils.WEB_APP_KEYSTORE_PASSWORD_KEY).getCredential());
    assertArrayEquals(trustpass, provider.getCredentialEntry(
        WebAppUtils.WEB_APP_TRUSTSTORE_PASSWORD_KEY).getCredential());
    return conf;
  }

  public class TestBuilder extends HttpServer2.Builder {
    public String keypass;
    public String keystorePassword;
    public String truststorePassword;

    @Override
    public Builder trustStore(String location, String password, String type) {
      truststorePassword = password;
      return super.trustStore(location, password, type);
    }

    @Override
    public Builder keyStore(String location, String password, String type) {
      keystorePassword = password;
      return super.keyStore(location, password, type);
    }

    @Override
    public Builder keyPassword(String password) {
      keypass = password;
      return super.keyPassword(password);
    }
  }
}
