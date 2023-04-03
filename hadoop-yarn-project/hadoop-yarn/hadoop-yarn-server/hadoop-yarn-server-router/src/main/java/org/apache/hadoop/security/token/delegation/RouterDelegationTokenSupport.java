/*
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

package org.apache.hadoop.security.token.delegation;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;

/**
 * Workaround for serialization of {@link DelegationTokenInformation} through package access.
 * Future version of Hadoop should add this to DelegationTokenInformation itself.
 */
public final class RouterDelegationTokenSupport {

  private RouterDelegationTokenSupport() {
  }

  public static String encodeDelegationTokenInformation(DelegationTokenInformation token) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bos);
      WritableUtils.writeVInt(out, token.password.length);
      out.write(token.password);
      out.writeLong(token.renewDate);
      out.flush();
      byte[] tokenInfoBytes = bos.toByteArray();
      return Base64.getUrlEncoder().encodeToString(tokenInfoBytes);
    } catch (IOException ex) {
      throw new RuntimeException("Failed to encode token.", ex);
    }
  }

  public static DelegationTokenInformation decodeDelegationTokenInformation(byte[] tokenBytes)
      throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(tokenBytes));
    DelegationTokenInformation token = new DelegationTokenInformation(0, null);
    int len = WritableUtils.readVInt(in);
    token.password = new byte[len];
    in.readFully(token.password);
    token.renewDate = in.readLong();
    return token;
  }
}
