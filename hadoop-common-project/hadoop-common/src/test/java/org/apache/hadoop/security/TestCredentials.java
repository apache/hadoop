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


package org.apache.hadoop.security;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collection;

import javax.crypto.KeyGenerator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestCredentials {
  private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA1";
  private static final File tmpDir = GenericTestUtils.getTestDir("mapred");

  @Before
  public void setUp() {
    tmpDir.mkdir();
  }

  @After
  public void tearDown() {
    tmpDir.delete();
  }

  @SuppressWarnings("unchecked")
  @Test
  public <T extends TokenIdentifier> void testReadWriteStorage()
  throws IOException, NoSuchAlgorithmException{
    // create tokenStorage Object
    Credentials ts = new Credentials();

    Token<T> token1 = new Token();
    Token<T> token2 = new Token();
    Text service1 = new Text("service1");
    Text service2 = new Text("service2");
    Collection<Text> services = new ArrayList<Text>();

    services.add(service1);
    services.add(service2);

    token1.setService(service1);
    token2.setService(service2);
    ts.addToken(new Text("sometoken1"), token1);
    ts.addToken(new Text("sometoken2"), token2);

    // create keys and put it in
    final KeyGenerator kg = KeyGenerator.getInstance(DEFAULT_HMAC_ALGORITHM);
    String alias = "alias";
    Map<Text, byte[]> m = new HashMap<Text, byte[]>(10);
    for(int i=0; i<10; i++) {
      Key key = kg.generateKey();
      m.put(new Text(alias+i), key.getEncoded());
      ts.addSecretKey(new Text(alias+i), key.getEncoded());
    }

    // create file to store
    File tmpFileName = new File(tmpDir, "tokenStorageTest");
    DataOutputStream dos =
      new DataOutputStream(new FileOutputStream(tmpFileName));
    ts.write(dos);
    dos.close();

    // open and read it back
    DataInputStream dis =
      new DataInputStream(new FileInputStream(tmpFileName));
    ts = new Credentials();
    ts.readFields(dis);
    dis.close();

    // get the tokens and compare the services
    Collection<Token<? extends TokenIdentifier>> list = ts.getAllTokens();
    assertEquals("getAllTokens should return collection of size 2",
        list.size(), 2);
    boolean foundFirst = false;
    boolean foundSecond = false;
    for (Token<? extends TokenIdentifier> token : list) {
      if (token.getService().equals(service1)) {
        foundFirst = true;
      }
      if (token.getService().equals(service2)) {
        foundSecond = true;
      }
    }
    assertTrue("Tokens for services service1 and service2 must be present",
        foundFirst && foundSecond);
    // compare secret keys
    int mapLen = m.size();
    assertEquals("wrong number of keys in the Storage",
        mapLen, ts.numberOfSecretKeys());
    for(Text a : m.keySet()) {
      byte [] kTS = ts.getSecretKey(a);
      byte [] kLocal = m.get(a);
      assertTrue("keys don't match for " + a,
          WritableComparator.compareBytes(kTS, 0, kTS.length, kLocal,
              0, kLocal.length)==0);
    }
    tmpFileName.delete();
  }

  @Test
  public void testBasicReadWriteProtoEmpty()
      throws IOException, NoSuchAlgorithmException {
    String testname ="testBasicReadWriteProtoEmpty";
    Credentials ts = new Credentials();
    writeCredentialsProto(ts, testname);
    Credentials ts2 = readCredentialsProto(testname);
    assertEquals("test empty tokens", 0, ts2.numberOfTokens());
    assertEquals("test empty keys", 0, ts2.numberOfSecretKeys());
  }

  @Test
  public void testBasicReadWriteProto()
      throws IOException, NoSuchAlgorithmException {
    String testname ="testBasicReadWriteProto";
    Text tok1 = new Text("token1");
    Text tok2 = new Text("token2");
    Text key1 = new Text("key1");
    Credentials ts = generateCredentials(tok1, tok2, key1);
    writeCredentialsProto(ts, testname);
    Credentials ts2 = readCredentialsProto(testname);
    assertCredentials(testname, tok1, key1, ts, ts2);
    assertCredentials(testname, tok2, key1, ts, ts2);
  }

  @Test
  public void testBasicReadWriteStreamEmpty()
      throws IOException, NoSuchAlgorithmException {
    String testname ="testBasicReadWriteStreamEmpty";
    Credentials ts = new Credentials();
    writeCredentialsStream(ts, testname);
    Credentials ts2 = readCredentialsStream(testname);
    assertEquals("test empty tokens", 0, ts2.numberOfTokens());
    assertEquals("test empty keys", 0, ts2.numberOfSecretKeys());
  }

  @Test
  public void testBasicReadWriteStream()
      throws IOException, NoSuchAlgorithmException {
    String testname ="testBasicReadWriteStream";
    Text tok1 = new Text("token1");
    Text tok2 = new Text("token2");
    Text key1 = new Text("key1");
    Credentials ts = generateCredentials(tok1, tok2, key1);
    writeCredentialsStream(ts, testname);
    Credentials ts2 = readCredentialsStream(testname);
    assertCredentials(testname, tok1, key1, ts, ts2);
    assertCredentials(testname, tok2, key1, ts, ts2);
  }

  @Test
  /**
   * Verify the suitability of read/writeProto for use with Writable interface.
   * This test uses only empty credentials.
   */
  public void testWritablePropertiesEmpty()
      throws IOException, NoSuchAlgorithmException {
    String testname ="testWritablePropertiesEmpty";
    Credentials ts = new Credentials();
    Credentials ts2 = new Credentials();
    writeCredentialsProtos(ts, ts2, testname);
    List<Credentials> clist = readCredentialsProtos(testname);
    assertEquals("test empty tokens 0", 0, clist.get(0).numberOfTokens());
    assertEquals("test empty keys 0", 0, clist.get(0).numberOfSecretKeys());
    assertEquals("test empty tokens 1", 0, clist.get(1).numberOfTokens());
    assertEquals("test empty keys 1", 0, clist.get(1).numberOfSecretKeys());
  }

  @Test
  /**
   * Verify the suitability of read/writeProto for use with Writable interface.
   */
  public void testWritableProperties()
      throws IOException, NoSuchAlgorithmException {
    String testname ="testWritableProperties";
    Text tok1 = new Text("token1");
    Text tok2 = new Text("token2");
    Text key1 = new Text("key1");
    Credentials ts = generateCredentials(tok1, tok2, key1);
    Text tok3 = new Text("token3");
    Text key2 = new Text("key2");
    Credentials ts2 = generateCredentials(tok1, tok3, key2);
    writeCredentialsProtos(ts, ts2, testname);
    List<Credentials> clist = readCredentialsProtos(testname);
    assertCredentials(testname, tok1, key1, ts, clist.get(0));
    assertCredentials(testname, tok2, key1, ts, clist.get(0));
    assertCredentials(testname, tok1, key2, ts2, clist.get(1));
    assertCredentials(testname, tok3, key2, ts2, clist.get(1));
  }

  private Credentials generateCredentials(Text t1, Text t2, Text t3)
      throws NoSuchAlgorithmException {
    Text kind = new Text("TESTTOK");
    byte[] id1 = {0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72};
    byte[] pass1 = {0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64};
    byte[] id2 = {0x68, 0x63, 0x64, 0x6d, 0x73, 0x68, 0x65, 0x68, 0x64, 0x71};
    byte[] pass2 = {0x6f, 0x60, 0x72, 0x72, 0x76, 0x6e, 0x71, 0x63};
    Credentials ts = new Credentials();
    generateToken(ts, id1, pass1, kind, t1);
    generateToken(ts, id2, pass2, kind, t2);
    generateKey(ts, t3);
    return ts;
  }

  private void assertCredentials(String tag, Text alias, Text keykey,
                                 Credentials a, Credentials b) {
    assertEquals(tag + ": test token count", a.numberOfTokens(),
                                             b.numberOfTokens());
    assertEquals(tag + ": test service", a.getToken(alias).getService(),
                                         b.getToken(alias).getService());
    assertEquals(tag + ": test kind", a.getToken(alias).getKind(),
                                      b.getToken(alias).getKind());
    assertTrue(tag + ": test password",
        Arrays.equals(a.getToken(alias).getPassword(),
                      b.getToken(alias).getPassword()));
    assertTrue(tag + ": test identifier",
        Arrays.equals(a.getToken(alias).getIdentifier(),
                      b.getToken(alias).getIdentifier()));
    assertEquals(tag + ": test number of keys", a.numberOfSecretKeys(),
                                                b.numberOfSecretKeys());
    assertTrue(tag + ":test key values", Arrays.equals(a.getSecretKey(keykey),
                                                       b.getSecretKey(keykey)));
  }

  private void writeCredentialsStream(Credentials creds, String filename)
      throws IOException, FileNotFoundException {
    DataOutputStream dos = new DataOutputStream(
        new FileOutputStream(new File(tmpDir, filename)));
    creds.writeTokenStorageToStream(dos);
  }

  private Credentials readCredentialsStream(String filename)
      throws IOException, FileNotFoundException {
    Credentials creds = new Credentials();
    DataInputStream dis = new DataInputStream(
        new FileInputStream(new File(tmpDir, filename)));
    creds.readTokenStorageStream(dis);
    return creds;
  }

  private void writeCredentialsProto(Credentials creds, String filename)
      throws IOException, FileNotFoundException {
    DataOutputStream dos = new DataOutputStream(
        new FileOutputStream(new File(tmpDir, filename)));
    creds.writeProto(dos);
  }

  private Credentials readCredentialsProto(String filename)
      throws IOException, FileNotFoundException {
    Credentials creds = new Credentials();
    DataInputStream dis = new DataInputStream(
        new FileInputStream(new File(tmpDir, filename)));
    creds.readProto(dis);
    return creds;
  }

  private void writeCredentialsProtos(Credentials c1, Credentials c2,
      String filename) throws IOException, FileNotFoundException {
    DataOutputStream dos = new DataOutputStream(
        new FileOutputStream(new File(tmpDir, filename)));
    c1.writeProto(dos);
    c2.writeProto(dos);
  }

  private List<Credentials> readCredentialsProtos(String filename)
      throws IOException, FileNotFoundException {
    Credentials c1 = new Credentials();
    Credentials c2 = new Credentials();
    DataInputStream dis = new DataInputStream(
        new FileInputStream(new File(tmpDir, filename)));
    c1.readProto(dis);
    c2.readProto(dis);
    List<Credentials> r = new ArrayList<Credentials>(2);
    r.add(0, c1);
    r.add(1, c2);
    return r;
  }

  private <T extends TokenIdentifier> void generateToken(
      Credentials creds, byte[] ident, byte[] pass, Text kind, Text service) {
    Token<T> token = new Token(ident, pass, kind, service);
    creds.addToken(service, token);
  }

  private void generateKey(Credentials creds, Text alias)
      throws NoSuchAlgorithmException {
    final KeyGenerator kg = KeyGenerator.getInstance(DEFAULT_HMAC_ALGORITHM);
    Key key = kg.generateKey();
    creds.addSecretKey(alias, key.getEncoded());
  }

  static Text secret[] = {
      new Text("secret1"),
      new Text("secret2"),
      new Text("secret3"),
      new Text("secret4")
  };
  static Text service[] = {
      new Text("service1"),
      new Text("service2"),
      new Text("service3"),
      new Text("service4")
  };
  static Token<?> token[] = {
      new Token<TokenIdentifier>(),
      new Token<TokenIdentifier>(),
      new Token<TokenIdentifier>(),
      new Token<TokenIdentifier>()
  };

  @Test
  public void addAll() {
    Credentials creds = new Credentials();
    creds.addToken(service[0], token[0]);
    creds.addToken(service[1], token[1]);
    creds.addSecretKey(secret[0], secret[0].getBytes());
    creds.addSecretKey(secret[1], secret[1].getBytes());

    Credentials credsToAdd = new Credentials();
    // one duplicate with different value, one new
    credsToAdd.addToken(service[0], token[3]);
    credsToAdd.addToken(service[2], token[2]);
    credsToAdd.addSecretKey(secret[0], secret[3].getBytes());
    credsToAdd.addSecretKey(secret[2], secret[2].getBytes());

    creds.addAll(credsToAdd);
    assertEquals(3, creds.numberOfTokens());
    assertEquals(3, creds.numberOfSecretKeys());
    // existing token & secret should be overwritten
    assertEquals(token[3], creds.getToken(service[0]));
    assertEquals(secret[3], new Text(creds.getSecretKey(secret[0])));
    // non-duplicate token & secret should be present
    assertEquals(token[1], creds.getToken(service[1]));
    assertEquals(secret[1], new Text(creds.getSecretKey(secret[1])));
    // new token & secret should be added
    assertEquals(token[2], creds.getToken(service[2]));
    assertEquals(secret[2], new Text(creds.getSecretKey(secret[2])));
  }

  @Test
  public void mergeAll() {
    Credentials creds = new Credentials();
    creds.addToken(service[0], token[0]);
    creds.addToken(service[1], token[1]);
    creds.addSecretKey(secret[0], secret[0].getBytes());
    creds.addSecretKey(secret[1], secret[1].getBytes());

    Credentials credsToAdd = new Credentials();
    // one duplicate with different value, one new
    credsToAdd.addToken(service[0], token[3]);
    credsToAdd.addToken(service[2], token[2]);
    credsToAdd.addSecretKey(secret[0], secret[3].getBytes());
    credsToAdd.addSecretKey(secret[2], secret[2].getBytes());

    creds.mergeAll(credsToAdd);
    assertEquals(3, creds.numberOfTokens());
    assertEquals(3, creds.numberOfSecretKeys());
    // existing token & secret should not be overwritten
    assertEquals(token[0], creds.getToken(service[0]));
    assertEquals(secret[0], new Text(creds.getSecretKey(secret[0])));
    // non-duplicate token & secret should be present
    assertEquals(token[1], creds.getToken(service[1]));
    assertEquals(secret[1], new Text(creds.getSecretKey(secret[1])));
    // new token & secret should be added
    assertEquals(token[2], creds.getToken(service[2]));
    assertEquals(secret[2], new Text(creds.getSecretKey(secret[2])));
  }

  @Test
  public void testAddTokensToUGI() {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("someone");
    Credentials creds = new Credentials();

    for (int i=0; i < service.length; i++) {
      creds.addToken(service[i], token[i]);
    }
    ugi.addCredentials(creds);

    creds = ugi.getCredentials();
    for (int i=0; i < service.length; i++) {
      assertSame(token[i], creds.getToken(service[i]));
    }
    assertEquals(service.length, creds.numberOfTokens());
  }
}
