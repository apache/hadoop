/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.diskbalancer;

import java.io.IOException;

/**
 * Disk Balancer Exceptions.
 */
public class DiskBalancerException extends IOException {
  /**
   * Results returned by the RPC layer of DiskBalancer.
   */
  public enum Result {
    DISK_BALANCER_NOT_ENABLED,
    INVALID_PLAN_VERSION,
    INVALID_PLAN,
    INVALID_PLAN_HASH,
    OLD_PLAN_SUBMITTED,
    DATANODE_ID_MISMATCH,
    MALFORMED_PLAN,
    PLAN_ALREADY_IN_PROGRESS,
    INVALID_VOLUME,
    INVALID_MOVE,
    INTERNAL_ERROR,
    NO_SUCH_PLAN,
    UNKNOWN_KEY,
    INVALID_NODE,
    DATANODE_STATUS_NOT_REGULAR,
  }

  private final Result result;

  /**
   * Constructs an {@code IOException} with the specified detail message.
   *
   * @param message The detail message (which is saved for later retrieval by
   *                the {@link #getMessage()} method)
   */
  public DiskBalancerException(String message, Result result) {
    super(message);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified detail message and
   * cause.
   * <p/>
   * <p> Note that the detail message associated with {@code cause} is
   * <i>not</i>
   * automatically incorporated into this exception's detail message.
   *
   * @param message The detail message (which is saved for later retrieval by
   *                the
   *                {@link #getMessage()} method)
   * @param cause   The cause (which is saved for later retrieval by the {@link
   *                #getCause()} method).  (A null value is permitted, and
   *                indicates that the cause is nonexistent or unknown.)
   */
  public DiskBalancerException(String message, Throwable cause, Result result) {
    super(message, cause);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified cause and a detail
   * message of {@code (cause==null ? null : cause.toString())} (which typically
   * contains the class and detail message of {@code cause}). This
   * constructor is useful for IO exceptions that are little more than
   * wrappers for other throwables.
   *
   * @param cause The cause (which is saved for later retrieval by the {@link
   *              #getCause()} method).  (A null value is permitted, and
   *              indicates
   *              that the cause is nonexistent or unknown.)
   */
  public DiskBalancerException(Throwable cause, Result result) {
    super(cause);
    this.result = result;
  }

  /**
   * Returns the result.
   * @return int
   */
  public Result getResult() {
    return result;
  }
}
