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

/**
 * <p><code>Token</code> is the security entity used by the framework
 * to verify authenticity of any resource.</p>
 */
@Public
@Stable
public interface Token {
  /**
   * Get the token identifier.
   * @return token identifier
   */
  @Public
  @Stable
  ByteBuffer getIdentifier();
  
  @Private
  @Stable
  void setIdentifier(ByteBuffer identifier);

  /**
   * Get the token password
   * @return token password
   */
  @Public
  @Stable
  ByteBuffer getPassword();
  
  @Private
  @Stable
  void setPassword(ByteBuffer password);

  /**
   * Get the token kind.
   * @return token kind
   */
  @Public
  @Stable
  String getKind();
  
  @Private
  @Stable
  void setKind(String kind);

  /**
   * Get the service to which the token is allocated.
   * @return service to which the token is allocated
   */
  @Public
  @Stable
  String getService();

  @Private
  @Stable
  void setService(String service);

}
