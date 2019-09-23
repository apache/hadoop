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

package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Thrown if there is a problem instantiating a KeyProvider or retrieving a key
 * using a KeyProvider object.
 */
@InterfaceAudience.Private
public class KeyProviderException extends AzureBlobFileSystemException {
  private static final long serialVersionUID = 1L;

  public KeyProviderException(String message) {
    super(message);
  }

  public KeyProviderException(String message, Throwable cause) {
    super(message);
  }

  public KeyProviderException(Throwable t) {
    super(t.getMessage());
  }
}
