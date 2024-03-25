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

package org.apache.hadoop.fs.azurebfs.security;

/**
 * Provides APIs to get encryptionKey from encryptionContext for a given path.
 */
public abstract class ContextEncryptionAdapter {

  /**
   * @return computed encryptionKey from server provided encryptionContext
   */
  public abstract String getEncodedKey();

  /**
   * @return computed encryptionKeySHA from server provided encryptionContext
   */
  public abstract String getEncodedKeySHA();

  /**
   * @return encryptionContext to be supplied in createPath API
   */
  public abstract String getEncodedContext();

  /**
   * Destroys all the encapsulated fields which are used for creating keys.
   */
  public abstract void destroy();
}
