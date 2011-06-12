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

package org.apache.hadoop.hdfs.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

@InterfaceAudience.Private
public class BlockAccessToken implements Writable {
  public static final BlockAccessToken DUMMY_TOKEN = new BlockAccessToken();
  private Text tokenID;
  private Text tokenAuthenticator;

  public BlockAccessToken() {
    this(new Text(), new Text());
  }

  public BlockAccessToken(Text tokenID, Text tokenAuthenticator) {
    this.tokenID = tokenID;
    this.tokenAuthenticator = tokenAuthenticator;
  }

  public Text getTokenID() {
    return tokenID;
  }

  public Text getTokenAuthenticator() {
    return tokenAuthenticator;
  }

  static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  /** {@inheritDoc} */
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof BlockAccessToken) {
      BlockAccessToken that = (BlockAccessToken) obj;
      return isEqual(this.tokenID, that.tokenID)
          && isEqual(this.tokenAuthenticator, that.tokenAuthenticator);
    }
    return false;
  }

  /** {@inheritDoc} */
  public int hashCode() {
    return tokenAuthenticator == null ? 0 : tokenAuthenticator.hashCode();
  }

  // ///////////////////////////////////////////////
  // Writable
  // ///////////////////////////////////////////////
  /**
   */
  public void write(DataOutput out) throws IOException {
    tokenID.write(out);
    tokenAuthenticator.write(out);
  }

  /**
   */
  public void readFields(DataInput in) throws IOException {
    tokenID.readFields(in);
    tokenAuthenticator.readFields(in);
  }

}
