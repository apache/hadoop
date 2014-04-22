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

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

public class TestKeyProvider {

  private static final String CIPHER = "AES";

  @Test
  public void testBuildVersionName() throws Exception {
    assertEquals("/a/b@3", KeyProvider.buildVersionName("/a/b", 3));
    assertEquals("/aaa@12", KeyProvider.buildVersionName("/aaa", 12));
  }

  @Test
  public void testParseVersionName() throws Exception {
    assertEquals("/a/b", KeyProvider.getBaseName("/a/b@3"));
    assertEquals("/aaa", KeyProvider.getBaseName("/aaa@112"));
    try {
      KeyProvider.getBaseName("no-slashes");
      assertTrue("should have thrown", false);
    } catch (IOException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testKeyMaterial() throws Exception {
    byte[] key1 = new byte[]{1,2,3,4};
    KeyProvider.KeyVersion obj = new KeyProvider.KeyVersion("key1@1", key1);
    assertEquals("key1@1", obj.getVersionName());
    assertArrayEquals(new byte[]{1,2,3,4}, obj.getMaterial());
  }

  @Test
  public void testMetadata() throws Exception {
    //Metadata without description
    DateFormat format = new SimpleDateFormat("y/m/d");
    Date date = format.parse("2013/12/25");
    KeyProvider.Metadata meta = new KeyProvider.Metadata("myCipher", 100, null,
        date, 123);
    assertEquals("myCipher", meta.getCipher());
    assertEquals(100, meta.getBitLength());
    assertNull(meta.getDescription());
    assertEquals(date, meta.getCreated());
    assertEquals(123, meta.getVersions());
    KeyProvider.Metadata second = new KeyProvider.Metadata(meta.serialize());
    assertEquals(meta.getCipher(), second.getCipher());
    assertEquals(meta.getBitLength(), second.getBitLength());
    assertNull(second.getDescription());
    assertEquals(meta.getCreated(), second.getCreated());
    assertEquals(meta.getVersions(), second.getVersions());
    int newVersion = second.addVersion();
    assertEquals(123, newVersion);
    assertEquals(124, second.getVersions());
    assertEquals(123, meta.getVersions());

    //Metadata with description
    format = new SimpleDateFormat("y/m/d");
    date = format.parse("2013/12/25");
    meta = new KeyProvider.Metadata("myCipher", 100,
        "description", date, 123);
    assertEquals("myCipher", meta.getCipher());
    assertEquals(100, meta.getBitLength());
    assertEquals("description", meta.getDescription());
    assertEquals(date, meta.getCreated());
    assertEquals(123, meta.getVersions());
    second = new KeyProvider.Metadata(meta.serialize());
    assertEquals(meta.getCipher(), second.getCipher());
    assertEquals(meta.getBitLength(), second.getBitLength());
    assertEquals(meta.getDescription(), second.getDescription());
    assertEquals(meta.getCreated(), second.getCreated());
    assertEquals(meta.getVersions(), second.getVersions());
    newVersion = second.addVersion();
    assertEquals(123, newVersion);
    assertEquals(124, second.getVersions());
    assertEquals(123, meta.getVersions());
  }

  @Test
  public void testOptions() throws Exception {
    Configuration conf = new Configuration();
    conf.set(KeyProvider.DEFAULT_CIPHER_NAME, "myCipher");
    conf.setInt(KeyProvider.DEFAULT_BITLENGTH_NAME, 512);
    KeyProvider.Options options = KeyProvider.options(conf);
    assertEquals("myCipher", options.getCipher());
    assertEquals(512, options.getBitLength());
    options.setCipher("yourCipher");
    options.setDescription("description");
    options.setBitLength(128);
    assertEquals("yourCipher", options.getCipher());
    assertEquals(128, options.getBitLength());
    assertEquals("description", options.getDescription());
    options = KeyProvider.options(new Configuration());
    assertEquals(KeyProvider.DEFAULT_CIPHER, options.getCipher());
    assertEquals(KeyProvider.DEFAULT_BITLENGTH, options.getBitLength());
  }

  @Test
  public void testUnnestUri() throws Exception {
    assertEquals(new Path("hdfs://nn.example.com/my/path"),
        KeyProvider.unnestUri(new URI("myscheme://hdfs@nn.example.com/my/path")));
    assertEquals(new Path("hdfs://nn/my/path?foo=bar&baz=bat#yyy"),
        KeyProvider.unnestUri(new URI("myscheme://hdfs@nn/my/path?foo=bar&baz=bat#yyy")));
    assertEquals(new Path("inner://hdfs@nn1.example.com/my/path"),
        KeyProvider.unnestUri(new URI("outer://inner@hdfs@nn1.example.com/my/path")));
    assertEquals(new Path("user:///"),
        KeyProvider.unnestUri(new URI("outer://user/")));
  }

  private static class MyKeyProvider extends KeyProvider {
    private String algorithm;
    private int size;
    private byte[] material;

    @Override
    public KeyVersion getKeyVersion(String versionName)
        throws IOException {
      return null;
    }

    @Override
    public List<String> getKeys() throws IOException {
      return null;
    }

    @Override
    public List<KeyVersion> getKeyVersions(String name)
        throws IOException {
      return null;
    }

    @Override
    public Metadata getMetadata(String name) throws IOException {
      return new Metadata(CIPHER, 128, "description", new Date(), 0);
    }

    @Override
    public KeyVersion createKey(String name, byte[] material,
        Options options) throws IOException {
      this.material = material;
      return null;
    }

    @Override
    public void deleteKey(String name) throws IOException {

    }

    @Override
    public KeyVersion rollNewVersion(String name, byte[] material)
        throws IOException {
      this.material = material;
      return null;
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    protected byte[] generateKey(int size, String algorithm)
        throws NoSuchAlgorithmException {
      this.size = size;
      this.algorithm = algorithm;
      return super.generateKey(size, algorithm);
    }
  }

  @Test
  public void testMaterialGeneration() throws Exception {
    MyKeyProvider kp = new MyKeyProvider();
    KeyProvider.Options options = new KeyProvider.Options(new Configuration());
    options.setCipher(CIPHER);
    options.setBitLength(128);
    kp.createKey("hello", options);
    Assert.assertEquals(128, kp.size);
    Assert.assertEquals(CIPHER, kp.algorithm);
    Assert.assertNotNull(kp.material);

    kp = new MyKeyProvider();
    kp.rollNewVersion("hello");
    Assert.assertEquals(128, kp.size);
    Assert.assertEquals(CIPHER, kp.algorithm);
    Assert.assertNotNull(kp.material);
  }
}
