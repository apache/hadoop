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
package org.apache.hadoop;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Indicates that a method has been passed illegal or invalid argument. This
 * exception is thrown instead of IllegalArgumentException to differentiate the
 * exception thrown in Hadoop implementation from the one thrown in JDK.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HadoopIllegalArgumentException extends IllegalArgumentException {
  private static final long serialVersionUID = 1L;
  
  /**
   * Constructs exception with the specified detail message. 
   * @param message detailed message.
   */
  public HadoopIllegalArgumentException(final String message) {
    super(message);
  }
}
