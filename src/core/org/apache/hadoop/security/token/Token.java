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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * The client-side form of the token.
 */
public class Token<T extends TokenIdentifier> implements Writable {
  private byte[] identifier;
  private byte[] password;
  private Text kind;
  private Text service;
  
  /**
   * Construct a token given a token identifier and a secret manager for the
   * type of the token identifier.
   * @param id the token identifier
   * @param mgr the secret manager
   */
  public Token(T id, SecretManager<T> mgr) {
    identifier = id.getBytes();
    password = mgr.createPassword(id);
    kind = id.getKind();
    service = new Text();
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
   * Get the token identifier
   * @return the token identifier
   */
  public byte[] getIdentifier() {
    return identifier;
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
  public Text getKind() {
    return kind;
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

  /** {@inheritDoc} */
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

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, identifier.length);
    out.write(identifier);
    WritableUtils.writeVInt(out, password.length);
    out.write(password);
    kind.write(out);
    service.write(out);
  }
}
