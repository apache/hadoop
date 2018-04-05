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
package org.apache.hadoop.ozone.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.File;
import java.io.IOException;

/**
 * The exception is thrown when file system state is inconsistent
 * and is not recoverable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class InconsistentStorageStateException extends IOException {
  private static final long serialVersionUID = 1L;

  public InconsistentStorageStateException(String descr) {
    super(descr);
  }

  public InconsistentStorageStateException(File dir, String descr) {
    super("Directory " + getFilePath(dir) + " is in an inconsistent state: "
        + descr);
  }

  private static String getFilePath(File dir) {
    try {
      return dir.getCanonicalPath();
    } catch (IOException e) {
    }
    return dir.getPath();
  }
}
