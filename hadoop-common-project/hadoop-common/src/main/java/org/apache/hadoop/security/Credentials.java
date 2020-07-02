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

import org.apache.hadoop.thirdparty.protobuf.ByteString;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.proto.SecurityProtos.CredentialsKVProto;
import org.apache.hadoop.security.proto.SecurityProtos.CredentialsProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that provides the facilities of reading and writing
 * secret keys and Tokens.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Credentials implements Writable {

  public enum SerializedFormat {
    WRITABLE((byte) 0x00),
    PROTOBUF((byte) 0x01);

    // Caching to avoid reconstructing the array each time.
    private static final SerializedFormat[] FORMATS = values();

    final byte value;

    SerializedFormat(byte val) {
      this.value = val;
    }

    public static SerializedFormat valueOf(int val) {
      try {
        return FORMATS[val];
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IllegalArgumentException("Unknown credential format: " + val);
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(Credentials.class);

  private  Map<Text, byte[]> secretKeysMap = new HashMap<Text, byte[]>();
  private  Map<Text, Token<? extends TokenIdentifier>> tokenMap =
      new HashMap<Text, Token<? extends TokenIdentifier>>();

  /**
   * Create an empty credentials instance.
   */
  public Credentials() {
  }

  /**
   * Create a copy of the given credentials.
   * @param credentials to copy
   */
  public Credentials(Credentials credentials) {
    this.addAll(credentials);
  }

  /**
   * Returns the Token object for the alias.
   * @param alias the alias for the Token
   * @return token for this alias
   */
  public Token<? extends TokenIdentifier> getToken(Text alias) {
    return tokenMap.get(alias);
  }

  /**
   * Add a token in the storage (in memory).
   * @param alias the alias for the key
   * @param t the token object
   */
  public void addToken(Text alias, Token<? extends TokenIdentifier> t) {
    if (t == null) {
      LOG.warn("Null token ignored for " + alias);
    } else if (tokenMap.put(alias, t) != null) {
      // Update private tokens
      Map<Text, Token<? extends TokenIdentifier>> tokensToAdd =
          new HashMap<>();
      for (Map.Entry<Text, Token<? extends TokenIdentifier>> e :
          tokenMap.entrySet()) {
        Token<? extends TokenIdentifier> token = e.getValue();
        if (token.isPrivateCloneOf(alias)) {
          tokensToAdd.put(e.getKey(), t.privateClone(token.getService()));
        }
      }
      tokenMap.putAll(tokensToAdd);
    }
  }

  /**
   * Return all the tokens in the in-memory map.
   */
  public Collection<Token<? extends TokenIdentifier>> getAllTokens() {
    return tokenMap.values();
  }

  /**
   * Returns an unmodifiable version of the full map of aliases to Tokens.
   */
  public Map<Text, Token<? extends TokenIdentifier>> getTokenMap() {
    return Collections.unmodifiableMap(tokenMap);
  }

  /**
   * @return number of Tokens in the in-memory map
   */
  public int numberOfTokens() {
    return tokenMap.size();
  }

  /**
   * Returns the key bytes for the alias.
   * @param alias the alias for the key
   * @return key for this alias
   */
  public byte[] getSecretKey(Text alias) {
    return secretKeysMap.get(alias);
  }

  /**
   * @return number of keys in the in-memory map
   */
  public int numberOfSecretKeys() {
    return secretKeysMap.size();
  }

  /**
   * Set the key for an alias.
   * @param alias the alias for the key
   * @param key the key bytes
   */
  public void addSecretKey(Text alias, byte[] key) {
    secretKeysMap.put(alias, key);
  }

  /**
   * Remove the key for a given alias.
   * @param alias the alias for the key
   */
  public void removeSecretKey(Text alias) {
    secretKeysMap.remove(alias);
  }

  /**
   * Return all the secret key entries in the in-memory map.
   */
  public List<Text> getAllSecretKeys() {
    List<Text> list = new java.util.ArrayList<Text>();
    list.addAll(secretKeysMap.keySet());

    return list;
  }

  /**
   * Returns an unmodifiable version of the full map of aliases to secret keys.
   */
  public Map<Text, byte[]> getSecretKeyMap() {
    return Collections.unmodifiableMap(secretKeysMap);
  }

  /**
   * Convenience method for reading a token storage file and loading its Tokens.
   * @param filename
   * @param conf
   * @throws IOException
   */
  public static Credentials readTokenStorageFile(Path filename,
                                                 Configuration conf)
  throws IOException {
    FSDataInputStream in = null;
    Credentials credentials = new Credentials();
    try {
      in = filename.getFileSystem(conf).open(filename);
      credentials.readTokenStorageStream(in);
      in.close();
      return credentials;
    } catch(IOException ioe) {
      throw IOUtils.wrapException(filename.toString(), "Credentials"
          + ".readTokenStorageFile", ioe);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
  }

  /**
   * Convenience method for reading a token storage file and loading its Tokens.
   * @param filename
   * @param conf
   * @throws IOException
   */
  public static Credentials readTokenStorageFile(File filename,
                                                 Configuration conf)
      throws IOException {
    DataInputStream in = null;
    Credentials credentials = new Credentials();
    try {
      in = new DataInputStream(new BufferedInputStream(
          Files.newInputStream(filename.toPath())));
      credentials.readTokenStorageStream(in);
      return credentials;
    } catch(IOException ioe) {
      throw new IOException("Exception reading " + filename, ioe);
    } finally {
      IOUtils.cleanupWithLogger(LOG, in);
    }
  }

  /**
   * Convenience method for reading a token from a DataInputStream.
   */
  public void readTokenStorageStream(DataInputStream in) throws IOException {
    byte[] magic = new byte[TOKEN_STORAGE_MAGIC.length];
    in.readFully(magic);
    if (!Arrays.equals(magic, TOKEN_STORAGE_MAGIC)) {
      throw new IOException("Bad header found in token storage.");
    }
    SerializedFormat format;
    try {
      format = SerializedFormat.valueOf(in.readByte());
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    }
    switch (format) {
    case WRITABLE:
      readFields(in);
      break;
    case PROTOBUF:
      readProto(in);
      break;
    default:
      throw new IOException("Unsupported format " + format);
    }
  }

  private static final byte[] TOKEN_STORAGE_MAGIC =
      "HDTS".getBytes(StandardCharsets.UTF_8);

  public void writeTokenStorageToStream(DataOutputStream os)
      throws IOException {
    // by default store in the oldest supported format for compatibility
    writeTokenStorageToStream(os, SerializedFormat.WRITABLE);
  }

  public void writeTokenStorageToStream(DataOutputStream os,
      SerializedFormat format) throws IOException {
    switch (format) {
    case WRITABLE:
      writeWritableOutputStream(os);
      break;
    case PROTOBUF:
      writeProtobufOutputStream(os);
      break;
    default:
      throw new IllegalArgumentException("Unsupported serialized format: "
          + format);
    }
  }

  private void writeWritableOutputStream(DataOutputStream os)
      throws IOException {
    os.write(TOKEN_STORAGE_MAGIC);
    os.write(SerializedFormat.WRITABLE.value);
    write(os);
  }

  private void writeProtobufOutputStream(DataOutputStream os)
      throws IOException {
    os.write(TOKEN_STORAGE_MAGIC);
    os.write(SerializedFormat.PROTOBUF.value);
    writeProto(os);
  }

  public void writeTokenStorageFile(Path filename,
                                    Configuration conf) throws IOException {
    // by default store in the oldest supported format for compatibility
    writeTokenStorageFile(filename, conf, SerializedFormat.WRITABLE);
  }

  public void writeTokenStorageFile(Path filename, Configuration conf,
      SerializedFormat format) throws IOException {
    try (FSDataOutputStream os =
             filename.getFileSystem(conf).create(filename)) {
      writeTokenStorageToStream(os, format);
    }
  }

  /**
   * Stores all the keys to DataOutput.
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
      WritableUtils.writeVInt(out, e.getValue().length);
      out.write(e.getValue());
    }
  }

  /**
   * Write contents of this instance as CredentialsProto message to DataOutput.
   * @param out
   * @throws IOException
   */
  void writeProto(DataOutput out) throws IOException {
    CredentialsProto.Builder storage = CredentialsProto.newBuilder();
    for (Map.Entry<Text, Token<? extends TokenIdentifier>> e :
                                                         tokenMap.entrySet()) {
      CredentialsKVProto.Builder kv = CredentialsKVProto.newBuilder().
          setAliasBytes(ByteString.copyFrom(
              e.getKey().getBytes(), 0, e.getKey().getLength())).
          setToken(ProtobufHelper.protoFromToken(e.getValue()));
      storage.addTokens(kv.build());
    }

    for(Map.Entry<Text, byte[]> e : secretKeysMap.entrySet()) {
      CredentialsKVProto.Builder kv = CredentialsKVProto.newBuilder().
          setAliasBytes(ByteString.copyFrom(
              e.getKey().getBytes(), 0, e.getKey().getLength())).
          setSecret(ByteString.copyFrom(e.getValue()));
      storage.addSecrets(kv.build());
    }
    storage.build().writeDelimitedTo((DataOutputStream)out);
  }

  /**
   * Populates keys/values from proto buffer storage.
   * @param in - stream ready to read a serialized proto buffer message
   */
  void readProto(DataInput in) throws IOException {
    CredentialsProto storage = CredentialsProto.parseDelimitedFrom((DataInputStream)in);
    for (CredentialsKVProto kv : storage.getTokensList()) {
      addToken(new Text(kv.getAliasBytes().toByteArray()),
               ProtobufHelper.tokenFromProto(kv.getToken()));
    }
    for (CredentialsKVProto kv : storage.getSecretsList()) {
      addSecretKey(new Text(kv.getAliasBytes().toByteArray()),
                   kv.getSecret().toByteArray());
    }
  }

  /**
   * Loads all the keys.
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
      int len = WritableUtils.readVInt(in);
      byte[] value = new byte[len];
      in.readFully(value);
      secretKeysMap.put(alias, value);
    }
  }

  /**
   * Copy all of the credentials from one credential object into another.
   * Existing secrets and tokens are overwritten.
   * @param other the credentials to copy
   */
  public void addAll(Credentials other) {
    addAll(other, true);
  }

  /**
   * Copy all of the credentials from one credential object into another.
   * Existing secrets and tokens are not overwritten.
   * @param other the credentials to copy
   */
  public void mergeAll(Credentials other) {
    addAll(other, false);
  }

  private void addAll(Credentials other, boolean overwrite) {
    for(Map.Entry<Text, byte[]> secret: other.secretKeysMap.entrySet()) {
      Text key = secret.getKey();
      if (!secretKeysMap.containsKey(key) || overwrite) {
        secretKeysMap.put(key, secret.getValue());
      }
    }
    for(Map.Entry<Text, Token<?>> token: other.tokenMap.entrySet()){
      Text key = token.getKey();
      if (!tokenMap.containsKey(key) || overwrite) {
        addToken(key, token.getValue());
      }
    }
  }
}
