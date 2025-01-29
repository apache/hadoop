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
package org.apache.hadoop.crypto.key;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKeyProviderDelegationTokenExtension {
  
  public static abstract class MockKeyProvider extends
      KeyProvider implements DelegationTokenExtension {

    public MockKeyProvider() {
      super(new Configuration(false));
    }
  }

  @Test
  public void testCreateExtension() throws Exception {
    Configuration conf = new Configuration();
    Credentials credentials = new Credentials();    
    KeyProvider kp = 
        new UserProvider.Factory().createProvider(new URI("user:///"), conf);
    KeyProviderDelegationTokenExtension kpDTE1 = 
        KeyProviderDelegationTokenExtension
        .createKeyProviderDelegationTokenExtension(kp);
    Assertions.assertNotNull(kpDTE1);
    Token<?>[] tokens = kpDTE1.addDelegationTokens("user", credentials);
    // Default implementation should return no tokens.
    Assertions.assertNotNull(tokens);
    Assertions.assertEquals(0, tokens.length);
    
    MockKeyProvider mock = mock(MockKeyProvider.class);
    Mockito.when(mock.getConf()).thenReturn(new Configuration());
    when(mock.getCanonicalServiceName()).thenReturn("cservice");
    when(mock.getDelegationToken("renewer")).thenReturn(
        new Token(null, null, new Text("kind"), new Text(
            "tservice"))
    );
    KeyProviderDelegationTokenExtension kpDTE2 =
        KeyProviderDelegationTokenExtension
        .createKeyProviderDelegationTokenExtension(mock);
    tokens = kpDTE2.addDelegationTokens("renewer", credentials);
    Assertions.assertNotNull(tokens);
    Assertions.assertEquals(1, tokens.length);
    Assertions.assertEquals("kind", tokens[0].getKind().toString());
    Assertions.assertEquals("tservice", tokens[0].getService().toString());
    Assertions.assertNotNull(credentials.getToken(new Text("cservice")));
  }

}
