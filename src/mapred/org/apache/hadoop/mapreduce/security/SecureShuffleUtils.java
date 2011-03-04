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
package org.apache.hadoop.mapreduce.security;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.record.Utils;

/**
 * 
 * utilities for generating kyes, hashes and verifying them for shuffle
 *
 */
public class SecureShuffleUtils {
  public static final String HTTP_HEADER_URL_HASH = "UrlHash";
  public static final String HTTP_HEADER_REPLY_URL_HASH = "ReplyHash";
  public static KeyGenerator kg = null;
  public static String DEFAULT_ALG="HmacSHA1";
  
  private SecretKeySpec secretKey;
  private Mac mac;
  
  /**
   * static generate keys
   * @return new encoded key
   * @throws NoSuchAlgorithmException
   */
  public static byte[] getNewEncodedKey() throws NoSuchAlgorithmException{
    SecretKeySpec key = generateKey(DEFAULT_ALG);
    return key.getEncoded();
  }
  
  private static SecretKeySpec generateKey(String alg) throws NoSuchAlgorithmException {
    if(kg==null) {
      kg = KeyGenerator.getInstance(alg);
    }
    return (SecretKeySpec) kg.generateKey();
  }

  /**
   * Create a util object with alg and key
   * @param sKeyEncoded
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeyException
   */
  public SecureShuffleUtils(byte [] sKeyEncoded) 
  throws  IOException{
    secretKey = new SecretKeySpec(sKeyEncoded, SecureShuffleUtils.DEFAULT_ALG);
    try {
      mac = Mac.getInstance(DEFAULT_ALG);
      mac.init(secretKey);
    } catch (NoSuchAlgorithmException nae) {
      throw new IOException(nae);
    } catch( InvalidKeyException ie) {
      throw new IOException(ie);
    }
  }
  
  /** 
   * get key as byte[]
   * @return encoded key
   */
  public byte [] getEncodedKey() {
    return secretKey.getEncoded();
  }
  
  /**
   * Base64 encoded hash of msg
   * @param msg
   */
  public String generateHash(byte[] msg) {
    return new String(Base64.encodeBase64(generateByteHash(msg)));
  }
  
  /**
   * calculate hash of msg
   * @param msg
   * @return
   */
  private byte[] generateByteHash(byte[] msg) {
    return mac.doFinal(msg);
  }
  
  /**
   * verify that hash equals to HMacHash(msg)
   * @param newHash
   * @return true if is the same
   */
  private boolean verifyHash(byte[] hash, byte[] msg) {
    byte[] msg_hash = generateByteHash(msg);
    return Utils.compareBytes(msg_hash, 0, msg_hash.length, hash, 0, hash.length) == 0;
  }
  
  /**
   * Aux util to calculate hash of a String
   * @param enc_str
   * @return Base64 encodedHash
   * @throws IOException
   */
  public String hashFromString(String enc_str) 
  throws IOException {
    return generateHash(enc_str.getBytes()); 
  }
  
  /**
   * verify that base64Hash is same as HMacHash(msg)  
   * @param base64Hash (Base64 encoded hash)
   * @param msg
   * @throws IOException if not the same
   */
  public void verifyReply(String base64Hash, String msg)
  throws IOException {
    byte[] hash = Base64.decodeBase64(base64Hash.getBytes());
    
    boolean res = verifyHash(hash, msg.getBytes());
    
    if(res != true) {
      throw new IOException("Verification of the hashReply failed");
    }
  }
  
  /**
   * Shuffle specific utils - build string for encoding from URL
   * @param url
   * @return string for encoding
   */
  public static String buildMsgFrom(URL url) {
    return buildMsgFrom(url.getPath(), url.getQuery(), url.getPort());
  }
  /**
   * Shuffle specific utils - build string for encoding from URL
   * @param request
   * @return string for encoding
   */
  public static String buildMsgFrom(HttpServletRequest request ) {
    return buildMsgFrom(request.getRequestURI(), request.getQueryString(),
        request.getLocalPort());
  }
  /**
   * Shuffle specific utils - build string for encoding from URL
   * @param uri_path
   * @param uri_query
   * @return string for encoding
   */
  private static String buildMsgFrom(String uri_path, String uri_query, int port) {
    return String.valueOf(port) + uri_path + "?" + uri_query;
  }
  
  
  /**
   * byte array to Hex String
   * @param ba
   * @return string with HEX value of the key
   */
  public static String toHex(byte[] ba) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    for(byte b: ba) {
      ps.printf("%x", b);
    }
    return baos.toString();
  }
}
