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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * get/set, store/load security keys
 * key's value - byte[]
 * store/load from DataInput/DataOuptut
 *
 */
//@InterfaceAudience.Private
public class TokenStorage implements Writable {

  private static final Text JOB_TOKEN = new Text("ShuffleJobToken");

  private  Map<Text, byte[]> secretKeysMap = new HashMap<Text, byte[]>();
  private  Map<Text, Token<? extends TokenIdentifier>> tokenMap = 
    new HashMap<Text, Token<? extends TokenIdentifier>>(); 

  /**
   * returns the key value for the alias
   * @param alias
   * @return key for this alias
   */
  public byte[] getSecretKey(Text alias) {
    return secretKeysMap.get(alias);
  }
  
  /**
   * returns the key value for the alias
   * @param alias
   * @return token for this alias
   */
  Token<? extends TokenIdentifier> getToken(Text alias) {
    return tokenMap.get(alias);
  }
  
  void setToken(Text alias, Token<? extends TokenIdentifier> t) {
    tokenMap.put(alias, t);
  }
  
  /**
   * store job token
   * @param t
   */
  //@InterfaceAudience.Private
  public void setJobToken(Token<? extends TokenIdentifier> t) {
    setToken(JOB_TOKEN, t);
  }
  
  /**
   * 
   * @return job token
   */
  //@InterfaceAudience.Private
  public Token<? extends TokenIdentifier> getJobToken() {
    return getToken(JOB_TOKEN);
  }
  
  /**
   * 
   * @return all the tokens in the storage
   */
  public Collection<Token<? extends TokenIdentifier>> getAllTokens() {
    return tokenMap.values();
  }
  

  
  /**
   * 
   * @return number of keys
   */
  public int numberOfSecretKeys() {
    return secretKeysMap.size();
  }
  
  
  /**
   * set the key for an alias
   * @param alias
   * @param key
   */
  public void addSecretKey(Text alias, byte[] key) {
    secretKeysMap.put(alias, key);
  }
 
  /**
   * stores all the keys to DataOutput
   * @param out
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    // write out tokens first
    System.out.println("about to write out: token = " + tokenMap.size() + 
        "; sec = " + secretKeysMap.size());
    WritableUtils.writeVInt(out, tokenMap.size());
    for(Map.Entry<Text, Token<? extends TokenIdentifier>> e: tokenMap.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
    
    // now write out secret keys
    WritableUtils.writeVInt(out, secretKeysMap.size());
    for(Map.Entry<Text, byte[]> e : secretKeysMap.entrySet()) {
      e.getKey().write(out);
      WritableUtils.writeCompressedByteArray(out, e.getValue());  
    }
  }
  
  /**
   * loads all the keys
   * @param in
   * @throws IOException
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    secretKeysMap.clear();
    tokenMap.clear();
    
    int size = WritableUtils.readVInt(in);
    for(int i=0; i<size; i++) {
      Text alias = new Text();
      alias.readFields(in);
      Token<? extends TokenIdentifier> t = new Token<TokenIdentifier>();
      t.readFields(in);
      tokenMap.put(alias, t);
    }
    
    size = WritableUtils.readVInt(in);
    for(int i=0; i<size; i++) {
      Text alias = new Text();
      alias.readFields(in);
      byte[] key = WritableUtils.readCompressedByteArray(in);
      secretKeysMap.put(alias, key);
    }
  }
}
