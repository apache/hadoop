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

package org.apache.hadoop.yarn.api.records;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p><code>Token</code> is the security entity used by the framework
 * to verify authenticity of any resource.</p>
 */
@Public
@Stable
public abstract class Token {

  @Private
  @Unstable
  public static Token newInstance(byte[] identifier, String kind, byte[] password,
      String service) {
    Token token = Records.newRecord(Token.class);
    token.setIdentifier(ByteBuffer.wrap(identifier));
    token.setKind(kind);
    token.setPassword(ByteBuffer.wrap(password));
    token.setService(service);
    return token;
  }

  /**
   * Get the token identifier.
   * @return token identifier
   */
  @Public
  @Stable
  public abstract ByteBuffer getIdentifier();
  
  @Private
  @Unstable
  public abstract void setIdentifier(ByteBuffer identifier);

  /**
   * Get the token password
   * @return token password
   */
  @Public
  @Stable
  public abstract ByteBuffer getPassword();
  
  @Private
  @Unstable
  public abstract void setPassword(ByteBuffer password);

  /**
   * Get the token kind.
   * @return token kind
   */
  @Public
  @Stable
  public abstract String getKind();
  
  @Private
  @Unstable
  public abstract void setKind(String kind);

  /**
   * Get the service to which the token is allocated.
   * @return service to which the token is allocated
   */
  @Public
  @Stable
  public abstract String getService();

  @Private
  @Unstable
  public abstract void setService(String service);

}
