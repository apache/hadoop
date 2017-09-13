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
package org.apache.hadoop.crypto.key.kms;

import static org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProvider.Options;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

public class TestLoadBalancingKMSClientProvider {

  @Test
  public void testCreation() throws Exception {
    Configuration conf = new Configuration();
    KeyProvider kp = new KMSClientProvider.Factory().createProvider(new URI(
        "kms://http@host1/kms/foo"), conf);
    assertTrue(kp instanceof KMSClientProvider);
    assertEquals("http://host1/kms/foo/v1/",
        ((KMSClientProvider) kp).getKMSUrl());

    kp = new KMSClientProvider.Factory().createProvider(new URI(
        "kms://http@host1;host2;host3/kms/foo"), conf);
    assertTrue(kp instanceof LoadBalancingKMSClientProvider);
    KMSClientProvider[] providers =
        ((LoadBalancingKMSClientProvider) kp).getProviders();
    assertEquals(3, providers.length);
    assertEquals(Sets.newHashSet("http://host1/kms/foo/v1/",
        "http://host2/kms/foo/v1/",
        "http://host3/kms/foo/v1/"),
        Sets.newHashSet(providers[0].getKMSUrl(),
            providers[1].getKMSUrl(),
            providers[2].getKMSUrl()));

    kp = new KMSClientProvider.Factory().createProvider(new URI(
        "kms://http@host1;host2;host3:9600/kms/foo"), conf);
    assertTrue(kp instanceof LoadBalancingKMSClientProvider);
    providers =
        ((LoadBalancingKMSClientProvider) kp).getProviders();
    assertEquals(3, providers.length);
    assertEquals(Sets.newHashSet("http://host1:9600/kms/foo/v1/",
        "http://host2:9600/kms/foo/v1/",
        "http://host3:9600/kms/foo/v1/"),
        Sets.newHashSet(providers[0].getKMSUrl(),
            providers[1].getKMSUrl(),
            providers[2].getKMSUrl()));
  }

  @Test
  public void testLoadBalancing() throws Exception {
    Configuration conf = new Configuration();
    KMSClientProvider p1 = mock(KMSClientProvider.class);
    when(p1.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenReturn(
            new KMSClientProvider.KMSKeyVersion("p1", "v1", new byte[0]));
    KMSClientProvider p2 = mock(KMSClientProvider.class);
    when(p2.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenReturn(
            new KMSClientProvider.KMSKeyVersion("p2", "v2", new byte[0]));
    KMSClientProvider p3 = mock(KMSClientProvider.class);
    when(p3.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenReturn(
            new KMSClientProvider.KMSKeyVersion("p3", "v3", new byte[0]));
    KeyProvider kp = new LoadBalancingKMSClientProvider(
        new KMSClientProvider[] { p1, p2, p3 }, 0, conf);
    assertEquals("p1", kp.createKey("test1", new Options(conf)).getName());
    assertEquals("p2", kp.createKey("test2", new Options(conf)).getName());
    assertEquals("p3", kp.createKey("test3", new Options(conf)).getName());
    assertEquals("p1", kp.createKey("test4", new Options(conf)).getName());
  }

  @Test
  public void testLoadBalancingWithFailure() throws Exception {
    Configuration conf = new Configuration();
    KMSClientProvider p1 = mock(KMSClientProvider.class);
    when(p1.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenReturn(
            new KMSClientProvider.KMSKeyVersion("p1", "v1", new byte[0]));
    when(p1.getKMSUrl()).thenReturn("p1");
    // This should not be retried
    KMSClientProvider p2 = mock(KMSClientProvider.class);
    when(p2.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenThrow(new NoSuchAlgorithmException("p2"));
    when(p2.getKMSUrl()).thenReturn("p2");
    KMSClientProvider p3 = mock(KMSClientProvider.class);
    when(p3.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenReturn(
            new KMSClientProvider.KMSKeyVersion("p3", "v3", new byte[0]));
    when(p3.getKMSUrl()).thenReturn("p3");
    // This should be retried
    KMSClientProvider p4 = mock(KMSClientProvider.class);
    when(p4.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenThrow(new IOException("p4"));
    when(p4.getKMSUrl()).thenReturn("p4");
    KeyProvider kp = new LoadBalancingKMSClientProvider(
        new KMSClientProvider[] { p1, p2, p3, p4 }, 0, conf);

    assertEquals("p1", kp.createKey("test4", new Options(conf)).getName());
    // Exceptions other than IOExceptions will not be retried
    try {
      kp.createKey("test1", new Options(conf)).getName();
      fail("Should fail since its not an IOException");
    } catch (Exception e) {
      assertTrue(e instanceof NoSuchAlgorithmException);
    }
    assertEquals("p3", kp.createKey("test2", new Options(conf)).getName());
    // IOException will trigger retry in next provider
    assertEquals("p1", kp.createKey("test3", new Options(conf)).getName());
  }

  @Test
  public void testLoadBalancingWithAllBadNodes() throws Exception {
    Configuration conf = new Configuration();
    KMSClientProvider p1 = mock(KMSClientProvider.class);
    when(p1.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenThrow(new IOException("p1"));
    KMSClientProvider p2 = mock(KMSClientProvider.class);
    when(p2.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenThrow(new IOException("p2"));
    KMSClientProvider p3 = mock(KMSClientProvider.class);
    when(p3.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenThrow(new IOException("p3"));
    KMSClientProvider p4 = mock(KMSClientProvider.class);
    when(p4.createKey(Mockito.anyString(), Mockito.any(Options.class)))
        .thenThrow(new IOException("p4"));
    when(p1.getKMSUrl()).thenReturn("p1");
    when(p2.getKMSUrl()).thenReturn("p2");
    when(p3.getKMSUrl()).thenReturn("p3");
    when(p4.getKMSUrl()).thenReturn("p4");
    KeyProvider kp = new LoadBalancingKMSClientProvider(
        new KMSClientProvider[] { p1, p2, p3, p4 }, 0, conf);
    try {
      kp.createKey("test3", new Options(conf)).getName();
      fail("Should fail since all providers threw an IOException");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
  }

  // copied from HttpExceptionUtils:

  // trick, riding on generics to throw an undeclared exception

  private static void throwEx(Throwable ex) {
    TestLoadBalancingKMSClientProvider.<RuntimeException>throwException(ex);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void throwException(Throwable ex)
      throws E {
    throw (E) ex;
  }

  private class MyKMSClientProvider extends KMSClientProvider {
    public MyKMSClientProvider(URI uri, Configuration conf) throws IOException {
      super(uri, conf);
    }

    @Override
    public EncryptedKeyVersion generateEncryptedKey(
        final String encryptionKeyName)
        throws IOException, GeneralSecurityException {
      throwEx(new AuthenticationException("bar"));
      return null;
    }

    @Override
    public KeyVersion decryptEncryptedKey(
        final EncryptedKeyVersion encryptedKeyVersion) throws IOException,
        GeneralSecurityException {
      throwEx(new AuthenticationException("bar"));
      return null;
    }

    @Override
    public KeyVersion createKey(final String name, final Options options)
        throws NoSuchAlgorithmException, IOException {
      throwEx(new AuthenticationException("bar"));
      return null;
    }

    @Override
    public KeyVersion rollNewVersion(final String name)
        throws NoSuchAlgorithmException, IOException {
      throwEx(new AuthenticationException("bar"));
      return null;
    }
  }

  @Test
  public void testClassCastException() throws Exception {
    Configuration conf = new Configuration();
    KMSClientProvider p1 = new MyKMSClientProvider(
        new URI("kms://http@host1/kms/foo"), conf);
    LoadBalancingKMSClientProvider kp = new LoadBalancingKMSClientProvider(
        new KMSClientProvider[] {p1}, 0, conf);
    try {
      kp.generateEncryptedKey("foo");
    } catch (IOException ioe) {
      assertTrue(ioe.getCause().getClass().getName().contains(
          "AuthenticationException"));
    }

    try {
      final KeyProviderCryptoExtension.EncryptedKeyVersion
          encryptedKeyVersion =
          mock(KeyProviderCryptoExtension.EncryptedKeyVersion.class);
      kp.decryptEncryptedKey(encryptedKeyVersion);
    } catch (IOException ioe) {
      assertTrue(ioe.getCause().getClass().getName().contains(
          "AuthenticationException"));
    }

    try {
      final KeyProvider.Options options = KeyProvider.options(conf);
      kp.createKey("foo", options);
    } catch (IOException ioe) {
      assertTrue(ioe.getCause().getClass().getName().contains(
          "AuthenticationException"));
    }

    try {
      kp.rollNewVersion("foo");
    } catch (IOException ioe) {
      assertTrue(ioe.getCause().getClass().getName().contains(
          "AuthenticationException"));
    }
  }

  /**
   * tests {@link LoadBalancingKMSClientProvider#warmUpEncryptedKeys(String...)}
   * error handling in case when all the providers throws {@link IOException}.
   * @throws Exception
   */
  @Test
  public void testWarmUpEncryptedKeysWhenAllProvidersFail() throws Exception {
    Configuration conf = new Configuration();
    KMSClientProvider p1 = mock(KMSClientProvider.class);
    String keyName = "key1";
    Mockito.doThrow(new IOException(new AuthorizationException("p1"))).when(p1)
        .warmUpEncryptedKeys(Mockito.anyString());
    KMSClientProvider p2 = mock(KMSClientProvider.class);
    Mockito.doThrow(new IOException(new AuthorizationException("p2"))).when(p2)
        .warmUpEncryptedKeys(Mockito.anyString());

    when(p1.getKMSUrl()).thenReturn("p1");
    when(p2.getKMSUrl()).thenReturn("p2");

    LoadBalancingKMSClientProvider kp = new LoadBalancingKMSClientProvider(
        new KMSClientProvider[] {p1, p2}, 0, conf);
    try {
      kp.warmUpEncryptedKeys(keyName);
      fail("Should fail since both providers threw IOException");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IOException);
    }
    Mockito.verify(p1, Mockito.times(1)).warmUpEncryptedKeys(keyName);
    Mockito.verify(p2, Mockito.times(1)).warmUpEncryptedKeys(keyName);
  }

  /**
   * tests {@link LoadBalancingKMSClientProvider#warmUpEncryptedKeys(String...)}
   * error handling in case atleast one provider succeeds.
   * @throws Exception
   */
  @Test
  public void testWarmUpEncryptedKeysWhenOneProviderSucceeds()
      throws Exception {
    Configuration conf = new Configuration();
    KMSClientProvider p1 = mock(KMSClientProvider.class);
    String keyName = "key1";
    Mockito.doThrow(new IOException(new AuthorizationException("p1"))).when(p1)
        .warmUpEncryptedKeys(Mockito.anyString());
    KMSClientProvider p2 = mock(KMSClientProvider.class);
    Mockito.doNothing().when(p2)
        .warmUpEncryptedKeys(Mockito.anyString());

    when(p1.getKMSUrl()).thenReturn("p1");
    when(p2.getKMSUrl()).thenReturn("p2");

    LoadBalancingKMSClientProvider kp = new LoadBalancingKMSClientProvider(
        new KMSClientProvider[] {p1, p2}, 0, conf);
    try {
      kp.warmUpEncryptedKeys(keyName);
    } catch (Exception e) {
      fail("Should not throw Exception since p2 doesn't throw Exception");
    }
    Mockito.verify(p1, Mockito.times(1)).warmUpEncryptedKeys(keyName);
    Mockito.verify(p2, Mockito.times(1)).warmUpEncryptedKeys(keyName);
  }
}
