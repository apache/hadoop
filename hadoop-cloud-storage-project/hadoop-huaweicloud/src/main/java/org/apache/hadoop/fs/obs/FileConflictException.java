/*
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

package org.apache.hadoop.fs.obs;

import java.io.IOException;

/**
 * OBS file conflict exception.
 */
class FileConflictException extends IOException {
  private static final long serialVersionUID = -897856973823710492L;

  /**
   * Constructs a <code>FileConflictException</code> with the specified detail
   * message. The string <code>s</code> can be retrieved later by the
   * <code>{@link Throwable#getMessage}</code>
   * method of class <code>java.lang.Throwable</code>.
   *
   * @param s the detail message.
   */
  FileConflictException(final String s) {
    super(s);
  }
}
