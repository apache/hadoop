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
package org.apache.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Contains either a value of type T, or an IOException.
 *
 * This can be useful as a return value for batch APIs that need granular
 * error reporting.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public class Fallible<T> {
  private final T val;
  private final IOException ioe;

  public Fallible(T val) {
    this.val = val;
    this.ioe = null;
  }

  public Fallible(IOException ioe) {
    this.val = null;
    this.ioe = ioe;
  }

  public T get() throws IOException {
    if (ioe != null) {
      throw new IOException(ioe);
    }
    return this.val;
  }
}
