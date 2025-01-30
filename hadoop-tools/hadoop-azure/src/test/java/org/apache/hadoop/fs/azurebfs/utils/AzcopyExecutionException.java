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

import java.io.IOException;

/**
 * Exception thrown when there is an error executing the Azcopy tool.
 * This exception is used only in test code to indicate issues with the Azcopy tool execution.
 * It provides a suggestion to delete the specified Azcopy tool directory and rerun the tests.
 */
public class AzcopyExecutionException extends IOException {
  private static final String SUGGESTION = "Try deleting the following azcopy tool directory and rerun tests: ";

  public AzcopyExecutionException(String message, String azcopyPath) {
    super(message + SUGGESTION + azcopyPath);
  }

  public AzcopyExecutionException(String message, String azcopyPath, Throwable cause) {
    super(message + SUGGESTION + azcopyPath, cause);
  }
}
