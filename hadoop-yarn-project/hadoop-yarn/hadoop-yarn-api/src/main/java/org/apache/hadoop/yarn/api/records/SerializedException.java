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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

@Private
@Unstable
public abstract class SerializedException {

  @Private
  @Unstable
  public static SerializedException newInstance(Throwable e) {
    SerializedException exception =
        Records.newRecord(SerializedException.class);
    exception.init(e);
    return exception;
  }

  /**
   * Constructs a new <code>SerializedException</code> with the specified detail
   * message and cause.
   */
  @Private
  @Unstable
  public abstract void init(String message, Throwable cause);

  /**
   * Constructs a new <code>SerializedException</code> with the specified detail
   * message.
   */
  @Private
  @Unstable
  public abstract void init(String message);

  /**
   * Constructs a new <code>SerializedException</code> with the specified cause.
   */
  @Private
  @Unstable
  public abstract void init(Throwable cause);

  /**
   * Get the detail message string of this exception.
   * @return the detail message string of this exception.
   */
  @Private
  @Unstable
  public abstract String getMessage();

  /**
   * Get the backtrace of this exception. 
   * @return the backtrace of this exception.
   */
  @Private
  @Unstable
  public abstract String getRemoteTrace();

  /**
   * Get the cause of this exception or null if the cause is nonexistent or
   * unknown.
   * @return the cause of this exception.
   */
  @Private
  @Unstable
  public abstract SerializedException getCause();

  /**
   * Deserialize the exception to a new Throwable. 
   * @return the Throwable form of this serialized exception.
   */
  @Private
  @Unstable
  public abstract Throwable deSerialize();

  private void stringify(StringBuilder sb) {
    sb.append(getMessage())
        .append("\n")
        .append(getRemoteTrace());
    final SerializedException cause = getCause();
    if (cause != null) {
      sb.append("Caused by: ");
      cause.stringify(sb);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(128);
    stringify(sb);
    return sb.toString();
  }
}