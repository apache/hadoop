/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.keys;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for HDDS Key Generator.
 */
public class TestHDDSKeyGenerator {
  private SecurityConfig config;

  @Before
  public void init() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,  GenericTestUtils.getTempPath("testpath"));
    config = new SecurityConfig(conf);
  }
  /**
   * In this test we verify that we are able to create a key pair, then get
   * bytes of that and use ASN1. parser to parse it back to a private key.
   * @throws NoSuchProviderException - On Error, due to missing Java
   * dependencies.
   * @throws NoSuchAlgorithmException - On Error,  due to missing Java
   * dependencies.
   */
  @Test
  public void testGenerateKey()
      throws NoSuchProviderException, NoSuchAlgorithmException {
    HDDSKeyGenerator keyGen = new HDDSKeyGenerator(config.getConfiguration());
    KeyPair keyPair = keyGen.generateKey();
    Assert.assertEquals(config.getKeyAlgo(),
        keyPair.getPrivate().getAlgorithm());
    PKCS8EncodedKeySpec keySpec =
        new PKCS8EncodedKeySpec(keyPair.getPrivate().getEncoded());
    Assert.assertEquals("PKCS#8", keySpec.getFormat());
  }

  /**
   * In this test we assert that size that we specified is used for Key
   * generation.
   * @throws NoSuchProviderException - On Error, due to missing Java
   * dependencies.
   * @throws NoSuchAlgorithmException - On Error,  due to missing Java
   * dependencies.
   */
  @Test
  public void testGenerateKeyWithSize() throws NoSuchProviderException,
      NoSuchAlgorithmException {
    HDDSKeyGenerator keyGen = new HDDSKeyGenerator(config.getConfiguration());
    KeyPair keyPair = keyGen.generateKey(4096);
    PublicKey publicKey = keyPair.getPublic();
    if(publicKey instanceof RSAPublicKey) {
      Assert.assertEquals(4096,
          ((RSAPublicKey)(publicKey)).getModulus().bitLength());
    }
  }
}