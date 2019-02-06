/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.common.helpers;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import java.io.IOException;

/**
 * Exceptions thrown from the Storage Container.
 */
public class StorageContainerException extends IOException {
  private ContainerProtos.Result result;

  /**
   * Constructs an {@code IOException} with {@code null}
   * as its error detail message.
   */
  public StorageContainerException(ContainerProtos.Result result) {
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified detail message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the {@link #getMessage()} method)
   * @param result - The result code
   */
  public StorageContainerException(String message,
      ContainerProtos.Result result) {
    super(message);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified detail message
   * and cause.
   * <p>
   * <p> Note that the detail message associated with {@code cause} is
   * <i>not</i> automatically incorporated into this exception's detail
   * message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the {@link #getMessage()} method)
   *
   * @param cause The cause (which is saved for later retrieval by the {@link
   * #getCause()} method).  (A null value is permitted, and indicates that the
   * cause is nonexistent or unknown.)
   *
   * @param result - The result code
   * @since 1.6
   */
  public StorageContainerException(String message, Throwable cause,
      ContainerProtos.Result result) {
    super(message, cause);
    this.result = result;
  }

  /**
   * Constructs an {@code IOException} with the specified cause and a
   * detail message of {@code (cause==null ? null : cause.toString())}
   * (which typically contains the class and detail message of {@code cause}).
   * This constructor is useful for IO exceptions that are little more
   * than wrappers for other throwables.
   *
   * @param cause The cause (which is saved for later retrieval by the {@link
   * #getCause()} method).  (A null value is permitted, and indicates that the
   * cause is nonexistent or unknown.)
   * @param result - The result code
   * @since 1.6
   */
  public StorageContainerException(Throwable cause, ContainerProtos.Result
      result) {
    super(cause);
    this.result = result;
  }

  /**
   * Returns Result.
   *
   * @return Result.
   */
  public ContainerProtos.Result getResult() {
    return result;
  }


}
