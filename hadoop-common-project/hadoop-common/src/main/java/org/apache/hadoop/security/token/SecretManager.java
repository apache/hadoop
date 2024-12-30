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

package org.apache.hadoop.security.token;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.StandbyException;


/**
 * The server-side secret manager for each token type.
 * @param <T> The type of the token identifier
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class SecretManager<T extends TokenIdentifier> {

  public static final Logger LOG = LoggerFactory.getLogger(SecretManager.class);
  /**
   * The token was invalid and the message explains why.
   */
  @SuppressWarnings("serial")
  @InterfaceStability.Evolving
  public static class InvalidToken extends IOException {
    public InvalidToken(String msg) { 
      super(msg);
    }
  }
  
  /**
   * Create the password for the given identifier.
   * identifier may be modified inside this method.
   * @param identifier the identifier to use
   * @return the new password
   */
  protected abstract byte[] createPassword(T identifier);
  
  /**
   * Retrieve the password for the given token identifier. Should check the date
   * or registry to make sure the token hasn't expired or been revoked. Returns 
   * the relevant password.
   * @param identifier the identifier to validate
   * @return the password to use
   * @throws InvalidToken the token was invalid
   */
  public abstract byte[] retrievePassword(T identifier)
      throws InvalidToken;
  
  /**
   * The same functionality with {@link #retrievePassword}, except that this 
   * method can throw a {@link RetriableException} or a {@link StandbyException}
   * to indicate that client can retry/failover the same operation because of 
   * temporary issue on the server side.
   * 
   * @param identifier the identifier to validate
   * @return the password to use
   * @throws InvalidToken the token was invalid
   * @throws StandbyException the server is in standby state, the client can
   *         try other servers
   * @throws RetriableException the token was invalid, and the server thinks 
   *         this may be a temporary issue and suggests the client to retry
   * @throws IOException to allow future exceptions to be added without breaking
   *         compatibility        
   */
  public byte[] retriableRetrievePassword(T identifier)
      throws InvalidToken, StandbyException, RetriableException, IOException {
    return retrievePassword(identifier);
  }
  
  /**
   * Create an empty token identifier.
   * @return the newly created empty token identifier
   */
  public abstract T createIdentifier();

  /**
   * No-op if the secret manager is available for reading tokens, throw a
   * StandbyException otherwise.
   * 
   * @throws StandbyException if the secret manager is not available to read
   *         tokens
   */
  public void checkAvailableForRead() throws StandbyException {
    // Default to being available for read.
  }

  private static final String SELECTED_ALGORITHM;
  private static final int SELECTED_LENGTH;

  static {
    Configuration conf = new Configuration();
    String algorithm = conf.get(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_KEY,
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_GENERATOR_ALGORITHM_DEFAULT);
    LOG.info("Selected hash algorithm: {}", algorithm);
    SELECTED_ALGORITHM = algorithm;
    int length = conf.getInt(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_KEY,
      CommonConfigurationKeysPublic.HADOOP_SECURITY_SECRET_MANAGER_KEY_LENGTH_DEFAULT);
    LOG.info("Selected hash key length:{}", length);
    SELECTED_LENGTH = length;
  }

  /**
   * A thread local store for the Macs.
   */
  private static final ThreadLocal<Mac> threadLocalMac =
    new ThreadLocal<Mac>(){
    @Override
    protected Mac initialValue() {
      try {
        return Mac.getInstance(SELECTED_ALGORITHM);
      } catch (NoSuchAlgorithmException nsa) {
        throw new IllegalArgumentException("Can't find " + SELECTED_ALGORITHM, nsa);
      }
    }
  };

  /**
   * Key generator to use.
   */
  private final KeyGenerator keyGen;
  {
    try {
      keyGen = KeyGenerator.getInstance(SELECTED_ALGORITHM);
      keyGen.init(SELECTED_LENGTH);
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException("Can't find " + SELECTED_ALGORITHM, nsa);
    }
  }

  /**
   * Generate a new random secret key.
   * @return the new key
   */
  protected SecretKey generateSecret() {
    SecretKey key;
    synchronized (keyGen) {
      key = keyGen.generateKey();
    }
    return key;
  }

  /**
   * Compute HMAC of the identifier using the secret key and return the 
   * output as password
   * @param identifier the bytes of the identifier
   * @param key the secret key
   * @return the bytes of the generated password
   */
  public static byte[] createPassword(byte[] identifier,
                                         SecretKey key) {
    Mac mac = threadLocalMac.get();
    try {
      mac.init(key);
    } catch (InvalidKeyException ike) {
      throw new IllegalArgumentException("Invalid key to HMAC computation", 
                                         ike);
    }
    return mac.doFinal(identifier);
  }
  
  /**
   * Convert the byte[] to a secret key
   * @param key the byte[] to create a secret key from
   * @return the secret key
   */
  protected static SecretKey createSecretKey(byte[] key) {
    return new SecretKeySpec(key, SELECTED_ALGORITHM);
  }
}
