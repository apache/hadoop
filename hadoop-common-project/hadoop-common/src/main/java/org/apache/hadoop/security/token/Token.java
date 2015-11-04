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

import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The client-side form of the token.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Token<T extends TokenIdentifier> implements Writable {
  public static final Log LOG = LogFactory.getLog(Token.class);
  
  private static Map<Text, Class<? extends TokenIdentifier>> tokenKindMap;
  
  private byte[] identifier;
  private byte[] password;
  private Text kind;
  private Text service;
  private TokenRenewer renewer;
  
  /**
   * Construct a token given a token identifier and a secret manager for the
   * type of the token identifier.
   * @param id the token identifier
   * @param mgr the secret manager
   */
  public Token(T id, SecretManager<T> mgr) {
    password = mgr.createPassword(id);
    identifier = id.getBytes();
    kind = id.getKind();
    service = new Text();
  }
 
  /**
   * Construct a token from the components.
   * @param identifier the token identifier
   * @param password the token's password
   * @param kind the kind of token
   * @param service the service for this token
   */
  public Token(byte[] identifier, byte[] password, Text kind, Text service) {
    this.identifier = (identifier == null)? new byte[0] : identifier;
    this.password = (password == null)? new byte[0] : password;
    this.kind = (kind == null)? new Text() : kind;
    this.service = (service == null)? new Text() : service;
  }

  /**
   * Default constructor
   */
  public Token() {
    identifier = new byte[0];
    password = new byte[0];
    kind = new Text();
    service = new Text();
  }

  /**
   * Clone a token.
   * @param other the token to clone
   */
  public Token(Token<T> other) {
    this.identifier = other.identifier;
    this.password = other.password;
    this.kind = other.kind;
    this.service = other.service;
  }

  /**
   * Get the token identifier's byte representation
   * @return the token identifier's byte representation
   */
  public byte[] getIdentifier() {
    return identifier;
  }
  
  private static Class<? extends TokenIdentifier>
      getClassForIdentifier(Text kind) {
    Class<? extends TokenIdentifier> cls = null;
    synchronized (Token.class) {
      if (tokenKindMap == null) {
        tokenKindMap = Maps.newHashMap();
        for (TokenIdentifier id : ServiceLoader.load(TokenIdentifier.class)) {
          tokenKindMap.put(id.getKind(), id.getClass());
        }
      }
      cls = tokenKindMap.get(kind);
    }
    if (cls == null) {
      LOG.warn("Cannot find class for token kind " + kind);
      return null;
    }
    return cls;
  }
  
  /**
   * Get the token identifier object, or null if it could not be constructed
   * (because the class could not be loaded, for example).
   * @return the token identifier, or null
   * @throws IOException 
   */
  @SuppressWarnings("unchecked")
  public T decodeIdentifier() throws IOException {
    Class<? extends TokenIdentifier> cls = getClassForIdentifier(getKind());
    if (cls == null) {
      return null;
    }
    TokenIdentifier tokenIdentifier = ReflectionUtils.newInstance(cls, null);
    ByteArrayInputStream buf = new ByteArrayInputStream(identifier);
    DataInputStream in = new DataInputStream(buf);  
    tokenIdentifier.readFields(in);
    in.close();
    return (T) tokenIdentifier;
  }
  
  /**
   * Get the token password/secret
   * @return the token password/secret
   */
  public byte[] getPassword() {
    return password;
  }
  
  /**
   * Get the token kind
   * @return the kind of the token
   */
  public synchronized Text getKind() {
    return kind;
  }

  /**
   * Set the token kind. This is only intended to be used by services that
   * wrap another service's token.
   * @param newKind
   */
  @InterfaceAudience.Private
  public synchronized void setKind(Text newKind) {
    kind = newKind;
    renewer = null;
  }

  /**
   * Get the service on which the token is supposed to be used
   * @return the service name
   */
  public Text getService() {
    return service;
  }
  
  /**
   * Set the service on which the token is supposed to be used
   * @param newService the service name
   */
  public void setService(Text newService) {
    service = newService;
  }

  /**
   * Indicates whether the token is a clone.  Used by HA failover proxy
   * to indicate a token should not be visible to the user via
   * UGI.getCredentials()
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class PrivateToken<T extends TokenIdentifier> extends Token<T> {
    public PrivateToken(Token<T> token) {
      super(token);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int len = WritableUtils.readVInt(in);
    if (identifier == null || identifier.length != len) {
      identifier = new byte[len];
    }
    in.readFully(identifier);
    len = WritableUtils.readVInt(in);
    if (password == null || password.length != len) {
      password = new byte[len];
    }
    in.readFully(password);
    kind.readFields(in);
    service.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, identifier.length);
    out.write(identifier);
    WritableUtils.writeVInt(out, password.length);
    out.write(password);
    kind.write(out);
    service.write(out);
  }

  /**
   * Generate a string with the url-quoted base64 encoded serialized form
   * of the Writable.
   * @param obj the object to serialize
   * @return the encoded string
   * @throws IOException
   */
  private static String encodeWritable(Writable obj) throws IOException {
    DataOutputBuffer buf = new DataOutputBuffer();
    obj.write(buf);
    Base64 encoder = new Base64(0, null, true);
    byte[] raw = new byte[buf.getLength()];
    System.arraycopy(buf.getData(), 0, raw, 0, buf.getLength());
    return encoder.encodeToString(raw);
  }
  
  /**
   * Modify the writable to the value from the newValue
   * @param obj the object to read into
   * @param newValue the string with the url-safe base64 encoded bytes
   * @throws IOException
   */
  private static void decodeWritable(Writable obj, 
                                     String newValue) throws IOException {
    Base64 decoder = new Base64(0, null, true);
    DataInputBuffer buf = new DataInputBuffer();
    byte[] decoded = decoder.decode(newValue);
    buf.reset(decoded, decoded.length);
    obj.readFields(buf);
  }

  /**
   * Encode this token as a url safe string
   * @return the encoded string
   * @throws IOException
   */
  public String encodeToUrlString() throws IOException {
    return encodeWritable(this);
  }
  
  /**
   * Decode the given url safe string into this token.
   * @param newValue the encoded string
   * @throws IOException
   */
  public void decodeFromUrlString(String newValue) throws IOException {
    decodeWritable(this, newValue);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object right) {
    if (this == right) {
      return true;
    } else if (right == null || getClass() != right.getClass()) {
      return false;
    } else {
      Token<T> r = (Token<T>) right;
      return Arrays.equals(identifier, r.identifier) &&
             Arrays.equals(password, r.password) &&
             kind.equals(r.kind) &&
             service.equals(r.service);
    }
  }
  
  @Override
  public int hashCode() {
    return WritableComparator.hashBytes(identifier, identifier.length);
  }
  
  private static void addBinaryBuffer(StringBuilder buffer, byte[] bytes) {
    for (int idx = 0; idx < bytes.length; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        buffer.append(' ');
      }
      String num = Integer.toHexString(0xff & bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        buffer.append('0');
      }
      buffer.append(num);
    }
  }
  
  private void identifierToString(StringBuilder buffer) {
    T id = null;
    try {
      id = decodeIdentifier();
    } catch (IOException e) {
      // handle in the finally block
    } finally {
      if (id != null) {
        buffer.append("(").append(id).append(")");
      } else {
        addBinaryBuffer(buffer, identifier);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("Kind: ");
    buffer.append(kind.toString());
    buffer.append(", Service: ");
    buffer.append(service.toString());
    buffer.append(", Ident: ");
    identifierToString(buffer);
    return buffer.toString();
  }
  
  private static ServiceLoader<TokenRenewer> renewers =
      ServiceLoader.load(TokenRenewer.class);

  private synchronized TokenRenewer getRenewer() throws IOException {
    if (renewer != null) {
      return renewer;
    }
    renewer = TRIVIAL_RENEWER;
    synchronized (renewers) {
      for (TokenRenewer canidate : renewers) {
        if (canidate.handleKind(this.kind)) {
          renewer = canidate;
          return renewer;
        }
      }
    }
    LOG.warn("No TokenRenewer defined for token kind " + this.kind);
    return renewer;
  }

  /**
   * Is this token managed so that it can be renewed or cancelled?
   * @return true, if it can be renewed and cancelled.
   */
  public boolean isManaged() throws IOException {
    return getRenewer().isManaged(this);
  }

  /**
   * Renew this delegation token
   * @return the new expiration time
   * @throws IOException
   * @throws InterruptedException
   */
  public long renew(Configuration conf
                    ) throws IOException, InterruptedException {
    return getRenewer().renew(this, conf);
  }
  
  /**
   * Cancel this delegation token
   * @throws IOException
   * @throws InterruptedException
   */
  public void cancel(Configuration conf
                     ) throws IOException, InterruptedException {
    getRenewer().cancel(this, conf);
  }
  
  /**
   * A trivial renewer for token kinds that aren't managed. Sub-classes need
   * to implement getKind for their token kind.
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public static class TrivialRenewer extends TokenRenewer {
    
    // define the kind for this renewer
    protected Text getKind() {
      return null;
    }

    @Override
    public boolean handleKind(Text kind) {
      return kind.equals(getKind());
    }

    @Override
    public boolean isManaged(Token<?> token) {
      return false;
    }

    @Override
    public long renew(Token<?> token, Configuration conf) {
      throw new UnsupportedOperationException("Token renewal is not supported "+
                                              " for " + token.kind + " tokens");
    }

    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException,
        InterruptedException {
      throw new UnsupportedOperationException("Token cancel is not supported " +
          " for " + token.kind + " tokens");
    }

  }
  private static final TokenRenewer TRIVIAL_RENEWER = new TrivialRenewer();
}
