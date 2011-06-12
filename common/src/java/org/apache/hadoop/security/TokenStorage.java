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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * A class that provides the facilities of reading and writing 
 * secret keys and Tokens.
 */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Evolving
public class TokenStorage implements Writable {

  private  Map<Text, byte[]> secretKeysMap = new HashMap<Text, byte[]>();
  private  Map<Text, Token<? extends TokenIdentifier>> tokenMap = 
    new HashMap<Text, Token<? extends TokenIdentifier>>(); 

  /**
   * Returns the key bytes for the alias
   * @param alias the alias for the key
   * @return key for this alias
   */
  public byte[] getSecretKey(Text alias) {
    return secretKeysMap.get(alias);
  }
  
  /**
   * Returns the Token object for the alias
   * @param alias the alias for the Token
   * @return token for this alias
   */
  public Token<? extends TokenIdentifier> getToken(Text alias) {
    return tokenMap.get(alias);
  }
  
  /**
   * Add a token in the storage (in memory)
   * @param alias the alias for the key
   * @param t the token object
   */
  public void addToken(Text alias, Token<? extends TokenIdentifier> t) {
    tokenMap.put(alias, t);
  }
  
  /**
   * Return all the tokens in the in-memory map
   */
  public Collection<Token<? extends TokenIdentifier>> getAllTokens() {
    return tokenMap.values();
  }
  
  /**
   * @return number of Tokens in the in-memory map
   */
  public int numberOfTokens() {
    return tokenMap.size();
  }
  
  /**
   * @return number of keys in the in-memory map
   */
  public int numberOfSecretKeys() {
    return secretKeysMap.size();
  }
  
  /**
   * Set the key for an alias
   * @param alias the alias for the key
   * @param key the key bytes
   */
  public void addSecretKey(Text alias, byte[] key) {
    secretKeysMap.put(alias, key);
  }
 
  /**
   * Convenience method for reading a file, and loading the Tokens
   * therein in the passed UGI
   * @param filename
   * @param conf
   * @param ugi
   * @throws IOException
   */
  public static void readTokensAndLoadInUGI(String filename, Configuration conf, 
      UserGroupInformation ugi) throws IOException {
    Path localTokensFile = new Path (filename);
    FileSystem localFS = FileSystem.getLocal(conf);
    FSDataInputStream in = localFS.open(localTokensFile);
    TokenStorage ts = new TokenStorage();
    ts.readFields(in);
    for (Token<? extends TokenIdentifier> token : ts.getAllTokens()) {
      ugi.addToken(token);
    }
  }
  /**
   * Stores all the keys to DataOutput
   * @param out
   * @throws IOException
   */
  @Override
  public void write(DataOutput out) throws IOException {
    // write out tokens first
    WritableUtils.writeVInt(out, tokenMap.size());
    for(Map.Entry<Text, 
        Token<? extends TokenIdentifier>> e: tokenMap.entrySet()) {
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
   * Loads all the keys
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