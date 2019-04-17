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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Result of the verification whether the current cluster setup can
 * support all enabled EC policies.
 */
@InterfaceAudience.Private
public class ECTopologyVerifierResult {

  private final String resultMessage;
  private final boolean isSupported;

  public ECTopologyVerifierResult(boolean isSupported,
                                  String resultMessage) {
    this.resultMessage = resultMessage;
    this.isSupported = isSupported;
  }

  public String getResultMessage() {
    return resultMessage;
  }

  public boolean isSupported() {
    return isSupported;
  }
}
