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

package org.apache.hadoop.fs.azurebfs.utils;

/**
 * Enum EncryptionType to represent the level of encryption applied.
 * <ol>
 *   <li>GLOBAL_KEY: encrypt all files with the same client-provided key.</li>
 *   <li>ENCRYPTION_CONTEXT: uses client-provided implementation to generate keys.</li>
 *   <li>NONE: encryption handled entirely at server.</li>
 * </ol>
 */
public enum EncryptionType {
  GLOBAL_KEY,
  ENCRYPTION_CONTEXT,
  NONE
}
