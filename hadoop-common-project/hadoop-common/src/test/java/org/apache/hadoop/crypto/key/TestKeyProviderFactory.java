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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestKeyProviderFactory {

  private static File tmpDir;

  @Before
  public void setup() {
    tmpDir = new File(System.getProperty("test.build.data", "target"),
        UUID.randomUUID().toString());
    tmpDir.mkdirs();
  }

  @Test
  public void testFactory() throws Exception {
    Configuration conf = new Configuration();
    conf.set(KeyProviderFactory.KEY_PROVIDER_PATH,
        UserProvider.SCHEME_NAME + ":///," +
            JavaKeyStoreProvider.SCHEME_NAME + "://file" + tmpDir + "/test.jks");
    List<KeyProvider> providers = KeyProviderFactory.getProviders(conf);
    assertEquals(2, providers.size());
    assertEquals(UserProvider.class, providers.get(0).getClass());
    assertEquals(JavaKeyStoreProvider.class, providers.get(1).getClass());
    assertEquals(UserProvider.SCHEME_NAME +
        ":///", providers.get(0).toString());
    assertEquals(JavaKeyStoreProvider.SCHEME_NAME +
        "://file" + tmpDir + "/test.jks",
        providers.get(1).toString());
  }

  @Test
  public void testFactoryErrors() throws Exception {
    Configuration conf = new Configuration();
    conf.set(KeyProviderFactory.KEY_PROVIDER_PATH, "unknown:///");
    try {
      List<KeyProvider> providers = KeyProviderFactory.getProviders(conf);
      assertTrue("should throw!", false);
    } catch (IOException e) {
      assertEquals("No KeyProviderFactory for unknown:/// in " +
          KeyProviderFactory.KEY_PROVIDER_PATH,
          e.getMessage());
    }
  }

  @Test
  public void testUriErrors() throws Exception {
    Configuration conf = new Configuration();
    conf.set(KeyProviderFactory.KEY_PROVIDER_PATH, "unkn@own:/x/y");
    try {
      List<KeyProvider> providers = KeyProviderFactory.getProviders(conf);
      assertTrue("should throw!", false);
    } catch (IOException e) {
      assertEquals("Bad configuration of " +
          KeyProviderFactory.KEY_PROVIDER_PATH +
          " at unkn@own:/x/y", e.getMessage());
    }
  }

  static void checkSpecificProvider(Configuration conf,
                                   String ourUrl) throws Exception {
    KeyProvider provider = KeyProviderFactory.getProviders(conf).get(0);
    byte[] key1 = new byte[32];
    byte[] key2 = new byte[32];
    byte[] key3 = new byte[32];
    for(int i =0; i < key1.length; ++i) {
      key1[i] = (byte) i;
      key2[i] = (byte) (i * 2);
      key3[i] = (byte) (i * 3);
    }
    // ensure that we get nulls when the key isn't there
    assertEquals(null, provider.getKeyVersion("no-such-key"));
    assertEquals(null, provider.getMetadata("key"));
    // create a new key
    try {
      provider.createKey("key3", key3, KeyProvider.options(conf));
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // check the metadata for key3
    KeyProvider.Metadata meta = provider.getMetadata("key3");
    assertEquals(KeyProvider.DEFAULT_CIPHER, meta.getCipher());
    assertEquals(KeyProvider.DEFAULT_BITLENGTH, meta.getBitLength());
    assertEquals(1, meta.getVersions());
    // make sure we get back the right key
    assertArrayEquals(key3, provider.getCurrentKey("key3").getMaterial());
    assertEquals("key3@0", provider.getCurrentKey("key3").getVersionName());
    // try recreating key3
    try {
      provider.createKey("key3", key3, KeyProvider.options(conf));
      assertTrue("should throw", false);
    } catch (IOException e) {
      assertEquals("Key key3 already exists in " + ourUrl, e.getMessage());
    }
    provider.deleteKey("key3");
    try {
      provider.deleteKey("key3");
      assertTrue("should throw", false);
    } catch (IOException e) {
      assertEquals("Key key3 does not exist in " + ourUrl, e.getMessage());
    }
    provider.createKey("key3", key3, KeyProvider.options(conf));
    try {
      provider.createKey("key4", key3,
          KeyProvider.options(conf).setBitLength(8));
      assertTrue("should throw", false);
    } catch (IOException e) {
      assertEquals("Wrong key length. Required 8, but got 256", e.getMessage());
    }
    provider.createKey("key4", new byte[]{1},
        KeyProvider.options(conf).setBitLength(8));
    provider.rollNewVersion("key4", new byte[]{2});
    meta = provider.getMetadata("key4");
    assertEquals(2, meta.getVersions());
    assertArrayEquals(new byte[]{2},
        provider.getCurrentKey("key4").getMaterial());
    assertArrayEquals(new byte[]{1},
        provider.getKeyVersion("key4@0").getMaterial());
    assertEquals("key4@1", provider.getCurrentKey("key4").getVersionName());
    try {
      provider.rollNewVersion("key4", key1);
      assertTrue("should throw", false);
    } catch (IOException e) {
      assertEquals("Wrong key length. Required 8, but got 256", e.getMessage());
    }
    try {
      provider.rollNewVersion("no-such-key", key1);
      assertTrue("should throw", false);
    } catch (IOException e) {
      assertEquals("Key no-such-key not found", e.getMessage());
    }
    provider.flush();
    // get a new instance of the provider to ensure it was saved correctly
    provider = KeyProviderFactory.getProviders(conf).get(0);
    assertArrayEquals(new byte[]{2},
        provider.getCurrentKey("key4").getMaterial());
    assertArrayEquals(key3, provider.getCurrentKey("key3").getMaterial());
    assertEquals("key3@0", provider.getCurrentKey("key3").getVersionName());

    List<String> keys = provider.getKeys();
    assertTrue("Keys should have been returned.", keys.size() == 2);
    assertTrue("Returned Keys should have included key3.", keys.contains("key3"));
    assertTrue("Returned Keys should have included key4.", keys.contains("key4"));

    List<KeyVersion> kvl = provider.getKeyVersions("key3");
    assertTrue("KeyVersions should have been returned for key3.", kvl.size() == 1);
    assertTrue("KeyVersions should have included key3@0.", kvl.get(0).getVersionName().equals("key3@0"));
    assertArrayEquals(key3, kvl.get(0).getMaterial());
  }

  @Test
  public void testUserProvider() throws Exception {
    Configuration conf = new Configuration();
    final String ourUrl = UserProvider.SCHEME_NAME + ":///";
    conf.set(KeyProviderFactory.KEY_PROVIDER_PATH, ourUrl);
    checkSpecificProvider(conf, ourUrl);
    // see if the credentials are actually in the UGI
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
    assertArrayEquals(new byte[]{1},
        credentials.getSecretKey(new Text("key4@0")));
    assertArrayEquals(new byte[]{2},
        credentials.getSecretKey(new Text("key4@1")));
  }

  @Test
  public void testJksProvider() throws Exception {
    Configuration conf = new Configuration();
    final String ourUrl =
        JavaKeyStoreProvider.SCHEME_NAME + "://file" + tmpDir + "/test.jks";

    File file = new File(tmpDir, "test.jks");
    file.delete();
    conf.set(KeyProviderFactory.KEY_PROVIDER_PATH, ourUrl);
    checkSpecificProvider(conf, ourUrl);
    Path path = KeyProvider.unnestUri(new URI(ourUrl));
    FileSystem fs = path.getFileSystem(conf);
    FileStatus s = fs.getFileStatus(path);
    assertTrue(s.getPermission().toString().equals("rwx------"));
    assertTrue(file + " should exist", file.isFile());

    // check permission retention after explicit change
    fs.setPermission(path, new FsPermission("777"));
    checkPermissionRetention(conf, ourUrl, path);
  }

  public void checkPermissionRetention(Configuration conf, String ourUrl, Path path) throws Exception {
    KeyProvider provider = KeyProviderFactory.getProviders(conf).get(0);
    // let's add a new key and flush and check that permissions are still set to 777
    byte[] key = new byte[32];
    for(int i =0; i < key.length; ++i) {
      key[i] = (byte) i;
    }
    // create a new key
    try {
      provider.createKey("key5", key, KeyProvider.options(conf));
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    provider.flush();
    // get a new instance of the provider to ensure it was saved correctly
    provider = KeyProviderFactory.getProviders(conf).get(0);
    assertArrayEquals(key, provider.getCurrentKey("key5").getMaterial());

    FileSystem fs = path.getFileSystem(conf);
    FileStatus s = fs.getFileStatus(path);
    assertTrue("Permissions should have been retained from the preexisting keystore.", s.getPermission().toString().equals("rwxrwxrwx"));
  }

  @Test
  public void testJksProviderPasswordViaConfig() throws Exception {
    Configuration conf = new Configuration();
    final String ourUrl =
        JavaKeyStoreProvider.SCHEME_NAME + "://file" + tmpDir + "/test.jks";
    File file = new File(tmpDir, "test.jks");
    file.delete();
    try {
      conf.set(KeyProviderFactory.KEY_PROVIDER_PATH, ourUrl);
      conf.set(JavaKeyStoreProvider.KEYSTORE_PASSWORD_FILE_KEY,
          "javakeystoreprovider.password");
      KeyProvider provider = KeyProviderFactory.getProviders(conf).get(0);
      provider.createKey("key3", new byte[32], KeyProvider.options(conf));
      provider.flush();
    } catch (Exception ex) {
      Assert.fail("could not create keystore with password file");
    }
    KeyProvider provider = KeyProviderFactory.getProviders(conf).get(0);
    Assert.assertNotNull(provider.getCurrentKey("key3"));

    try {
      conf.set(JavaKeyStoreProvider.KEYSTORE_PASSWORD_FILE_KEY, "bar");
      KeyProviderFactory.getProviders(conf).get(0);
      Assert.fail("using non existing password file, it should fail");
    } catch (IOException ex) {
      //NOP
    }
    try {
      conf.set(JavaKeyStoreProvider.KEYSTORE_PASSWORD_FILE_KEY, "core-site.xml");
      KeyProviderFactory.getProviders(conf).get(0);
      Assert.fail("using different password file, it should fail");
    } catch (IOException ex) {
      //NOP
    }
    try {
      conf.unset(JavaKeyStoreProvider.KEYSTORE_PASSWORD_FILE_KEY);
      KeyProviderFactory.getProviders(conf).get(0);
      Assert.fail("No password file property, env not set, it should fail");
    } catch (IOException ex) {
      //NOP
    }
  }

}
