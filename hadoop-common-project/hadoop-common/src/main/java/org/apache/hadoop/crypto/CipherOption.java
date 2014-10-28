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
package org.apache.hadoop.crypto;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Used between client and server to negotiate the 
 * cipher suite, key and iv.
 */
@InterfaceAudience.Private
public class CipherOption {
  private final CipherSuite suite;
  private final byte[] inKey;
  private final byte[] inIv;
  private final byte[] outKey;
  private final byte[] outIv;
  
  public CipherOption(CipherSuite suite) {
    this(suite, null, null, null, null);
  }
  
  public CipherOption(CipherSuite suite, byte[] inKey, byte[] inIv, 
      byte[] outKey, byte[] outIv) {
    this.suite = suite;
    this.inKey = inKey;
    this.inIv = inIv;
    this.outKey = outKey;
    this.outIv = outIv;
  }
  
  public CipherSuite getCipherSuite() {
    return suite;
  }
  
  public byte[] getInKey() {
    return inKey;
  }
  
  public byte[] getInIv() {
    return inIv;
  }
  
  public byte[] getOutKey() {
    return outKey;
  }
  
  public byte[] getOutIv() {
    return outIv;
  }
}
