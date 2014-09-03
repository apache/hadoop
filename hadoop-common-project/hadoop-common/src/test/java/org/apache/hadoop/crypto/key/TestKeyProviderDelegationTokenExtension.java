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
import org.junit.Assert;
import org.junit.Test;
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
    Assert.assertNotNull(kpDTE1);
    // Default implementation should be a no-op and return null
    Assert.assertNull(kpDTE1.addDelegationTokens("user", credentials));
    
    MockKeyProvider mock = mock(MockKeyProvider.class);
    Mockito.when(mock.getConf()).thenReturn(new Configuration());
    when(mock.addDelegationTokens("renewer", credentials)).thenReturn(
        new Token<?>[]{new Token(null, null, new Text("kind"), new Text(
            "service"))}
    );
    KeyProviderDelegationTokenExtension kpDTE2 =
        KeyProviderDelegationTokenExtension
        .createKeyProviderDelegationTokenExtension(mock);
    Token<?>[] tokens = 
        kpDTE2.addDelegationTokens("renewer", credentials);
    Assert.assertNotNull(tokens);
    Assert.assertEquals("kind", tokens[0].getKind().toString());
    
  }

}
