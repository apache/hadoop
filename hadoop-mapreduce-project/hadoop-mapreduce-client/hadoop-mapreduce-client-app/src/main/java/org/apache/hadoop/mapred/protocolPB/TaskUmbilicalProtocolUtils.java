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

package org.apache.hadoop.mapred.protocolPB;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

import static org.apache.hadoop.ipc.internal.ShadedProtobufHelper.getRemoteException;

/**
 * Protocol helpers.
 * <p>The lambda expressions and invokers are very similar to that of ShadedProtobufHelper.ipc(),
 * but with broader exception catching.
 */
final class TaskUmbilicalProtocolUtils {

  private TaskUmbilicalProtocolUtils() {
  }

  /*
  Serialization helpers
   */

  /**
   * Serialize a Writable into a protobuf ByteString.
   * @param writable source
   * @return data written to bytes.
   * @throws IOException write failure.
   */
  static ByteString serialize(Writable writable)
      throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    writable.write(dob);
    return ByteString.copyFrom(dob.getData(), 0, dob.getLength());
  }

  /**
   * Deserialize specific writable from bytes in a protobuf string.
   * @param writable the writable instance which will be filled with the data
   * @param bytes bytes to unmarshall.
   * @param <T> type of the writable.
   * @return the writable.
   * @throws IOException read failure.
   */
  static <T extends Writable> T deserialize(
      T writable, ByteString bytes) throws IOException {
    DataInputBuffer dib = new DataInputBuffer();
    byte[] b = bytes.toByteArray();
    dib.reset(b, b.length);
    writable.readFields(dib);
    return writable;
  }

  /**
   * Serialize a task status.
   * @param status task to serialize
   * @return the serialized status, including a flag to indicate status type.
   * @throws IOException write failre.
   */
  static ByteString serializeTaskStatus(TaskStatus status) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    TaskStatus.writeTaskStatusForPB(dob, status);
    return ByteString.copyFrom(dob.getData(), 0, dob.getLength());
  }

  /**
   * Deserialize a byte string to a matching TaskStatus implementation.
   * @param bytes source bytes
   * @return task status
   * @throws IOException read failure.
   */
  static TaskStatus deserializeTaskStatus(ByteString bytes) throws IOException {
    DataInputBuffer dib = new DataInputBuffer();
    byte[] b = bytes.toByteArray();
    dib.reset(b, b.length);
    return TaskStatus.readTaskStatusFromPB(dib);
  }

  /*
  Service invocation with exception translation.
   */

  /**
   * Service invocation.
   * @param <Result> type of result.
   */
  @FunctionalInterface
  interface ServiceCallable<Result> {

    Result apply() throws IOException, ServiceException;
  }

  /**
   * Service invocation with void response.
   */
  @FunctionalInterface
  interface ServiceVoidCallable {

    void apply() throws IOException, ServiceException;
  }

  /**
   * Invoke a service.
   * @param callable callable
   * @param <Result> type of result
   * @return result
   * @throws UncheckedIOException wrapped IOE.
   */
  static <Result> Result service(ServiceCallable<Result> callable) {
    try {
      return callable.apply();
    } catch (ServiceException e) {
      throw new UncheckedIOException(getRemoteException(e));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Invoke a service with no result.
   * @param callable callable
   * @throws UncheckedIOException wrapped IOE.
   */
  static void service(ServiceVoidCallable callable) {
    service(() -> {
      callable.apply();
      return null;
    });
  }


  /**
   * Client side lambda expression.
   * @param <Result> type of result.
   */
  @FunctionalInterface
  interface ClientCallable<Result> {

    Result apply() throws IOException, InterruptedException;
  }

  /**
   * Client side lambda expression with void result.
   */
  @FunctionalInterface
  interface ClientVoidCallable {

    void apply() throws IOException, InterruptedException;
  }


  static <Result> Result translate(ClientCallable<Result> callable) throws ServiceException {
    try {
      return callable.apply();
    } catch (IOException | InterruptedException e) {
      throw new ServiceException(e);
    }
  }

  static void translate(ClientVoidCallable callable) throws ServiceException {
    translate(() -> {
      callable.apply();
      return null;
    });
  }

}
