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
import java.io.IOException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.KeyGenerator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.record.Utils;
import org.apache.hadoop.security.token.Token;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.hadoop.mapreduce.security.TokenStorage;

public class TestTokenStorage {
  private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA1";
  private static final File tmpDir =
    new File(System.getProperty("test.build.data", "/tmp"), "mapred");  
    
  @Before
  public void setUp() {
    tmpDir.mkdir();
  }

  @Test 
  public void testReadWriteStorage() throws IOException, NoSuchAlgorithmException{
    // create tokenStorage Object
    TokenStorage ts = new TokenStorage();
    
    // create a token
    JobTokenSecretManager jtSecretManager = new JobTokenSecretManager();
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text("fakeJobId"));
    Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>(identifier,
        jtSecretManager);
    // store it
    ts.setJobToken(jt);
    
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
    DataOutputStream dos = new DataOutputStream(new FileOutputStream(tmpFileName));
    ts.write(dos);
    dos.close();
    
    // open and read it back
    DataInputStream dis = new DataInputStream(new FileInputStream(tmpFileName));    
    ts = new TokenStorage();
    ts.readFields(dis);
    dis.close();
    
    // get the token and compare the passwords
    byte[] tp1 = ts.getJobToken().getPassword();
    byte[] tp2 = jt.getPassword();
    int comp = Utils.compareBytes(tp1, 0, tp1.length, tp2, 0, tp2.length);
    assertTrue("shuffleToken doesn't match", comp==0);
    
    // compare secret keys
    int mapLen = m.size();
    assertEquals("wrong number of keys in the Storage", mapLen, ts.numberOfSecretKeys());
    for(Text a : m.keySet()) {
      byte [] kTS = ts.getSecretKey(a);
      byte [] kLocal = m.get(a);
      assertTrue("keys don't match for " + a, 
          Utils.compareBytes(kTS, 0, kTS.length, kLocal, 0, kLocal.length)==0);
    }  
    
    assertEquals("All tokens should return collection of size 1", 
        ts.getAllTokens().size(), 1);
  }
 }
