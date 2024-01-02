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

package org.apache.hadoop.fs.azurebfs.extensions;

import javax.security.auth.Destroyable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.security.ABFSKey;

/**
 * This interface has two roles:<br>
 * <ul>
 *   <li>
 *     To create new encryptionContext from a given path: To be used in case of
 *     create file as there is no encryptionContext in remote server to refer to
 *     for encryptionKey creation.
 *   </li>
 *   <li>To create encryptionKey using encryptionContext.</li>
 * </ul>
 */
public interface EncryptionContextProvider extends Destroyable {
  /**
   * Initialize instance.
   *
   * @param configuration rawConfig instance
   * @param accountName Account Name (with domain)
   * @param fileSystem container name
   * @throws IOException error in initialization
   */
  void initialize(Configuration configuration, String accountName, String fileSystem) throws IOException;

  /**
   * Fetch encryption context for a given path.
   *
   * @param path file path from filesystem root
   * @return encryptionContext key
   * @throws IOException error in fetching encryption context
   */
  ABFSKey getEncryptionContext(String path) throws IOException;

  /**
   * Fetch encryption key in-exchange for encryption context.
   *
   * @param path file path from filesystem root
   * @param encryptionContext encryptionContext fetched from server
   * @return Encryption key
   * @throws IOException error in fetching encryption key
   */
  ABFSKey getEncryptionKey(String path, ABFSKey encryptionContext) throws IOException;
}
