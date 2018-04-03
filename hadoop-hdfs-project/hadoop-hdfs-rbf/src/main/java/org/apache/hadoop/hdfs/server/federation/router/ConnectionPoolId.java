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
package org.apache.hadoop.hdfs.server.federation.router;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Identifier for a connection for a user to a namenode.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ConnectionPoolId implements Comparable<ConnectionPoolId> {

  /** Namenode identifier. */
  private final String nnId;
  /** Information about the user. */
  private final UserGroupInformation ugi;

  /**
   * New connection pool identifier.
   *
   * @param ugi Information of the user issuing the request.
   * @param nnId Namenode address with port.
   */
  public ConnectionPoolId(final UserGroupInformation ugi, final String nnId) {
    this.nnId = nnId;
    this.ugi = ugi;
  }

  @Override
  public int hashCode() {
    int hash = new HashCodeBuilder(17, 31)
        .append(this.nnId)
        .append(this.ugi.toString())
        .append(this.getTokenIds())
        .toHashCode();
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ConnectionPoolId) {
      ConnectionPoolId other = (ConnectionPoolId) o;
      if (!this.nnId.equals(other.nnId)) {
        return false;
      }
      if (!this.ugi.toString().equals(other.ugi.toString())) {
        return false;
      }
      String thisTokens = this.getTokenIds().toString();
      String otherTokens = other.getTokenIds().toString();
      return thisTokens.equals(otherTokens);
    }
    return false;
  }

  @Override
  public String toString() {
    return this.ugi + " " + this.getTokenIds() + "->" + this.nnId;
  }

  @Override
  public int compareTo(ConnectionPoolId other) {
    int ret = this.nnId.compareTo(other.nnId);
    if (ret == 0) {
      ret = this.ugi.toString().compareTo(other.ugi.toString());
    }
    if (ret == 0) {
      String thisTokens = this.getTokenIds().toString();
      String otherTokens = other.getTokenIds().toString();
      ret = thisTokens.compareTo(otherTokens);
    }
    return ret;
  }

  @VisibleForTesting
  UserGroupInformation getUgi() {
    return this.ugi;
  }

  /**
   * Get the token identifiers for this connection.
   * @return List with the token identifiers.
   */
  private List<String> getTokenIds() {
    List<String> tokenIds = new ArrayList<>();
    Collection<Token<? extends TokenIdentifier>> tokens = this.ugi.getTokens();
    for (Token<? extends TokenIdentifier> token : tokens) {
      byte[] tokenIdBytes = token.getIdentifier();
      String tokenId = Arrays.toString(tokenIdBytes);
      tokenIds.add(tokenId);
    }
    Collections.sort(tokenIds);
    return tokenIds;
  }
}