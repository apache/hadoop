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

package org.apache.hadoop.fs.bos.exceptions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

/**
 * Thrown if there is a problem communicating with Baidu BOS.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BosException extends IOException {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a BosException with the specified cause.
   *
   * @param t the cause of this exception
   */
  public BosException(Throwable t) {
    super(t);
  }

  /**
   * Constructs a BosException with the specified message.
   *
   * @param message the detail message
   */
  public BosException(String message) {
    super(message);
  }
}
