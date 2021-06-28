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

package org.apache.hadoop.fs.azurebfs.utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.fastpath.driver.FastpathDriver;
import com.azure.storage.fastpath.FastpathConnection;
import com.azure.storage.fastpath.exceptions.FastpathException;
import com.azure.storage.fastpath.exceptions.FastpathConnectionException;
import com.azure.storage.fastpath.exceptions.FastpathRequestException;
import com.azure.storage.fastpath.requestParameters.FastpathCloseRequestParams;
import com.azure.storage.fastpath.requestParameters.FastpathOpenRequestParams;
import com.azure.storage.fastpath.requestParameters.FastpathReadRequestParams;
import com.azure.storage.fastpath.responseProviders.FastpathCloseResponse;
import com.azure.storage.fastpath.responseProviders.FastpathOpenResponse;
import com.azure.storage.fastpath.responseProviders.FastpathReadResponse;

public class MockFastpathConnection
    extends FastpathConnection {

  int errStatus = 0;
  boolean mockRequestException = false;
  boolean mockConnectionException = false;

  protected static final Logger LOG = LoggerFactory.getLogger(
      MockFastpathConnection.class);

  private final static MockFastpathDriver
      nativeApiCaller = new MockFastpathDriver();

  static AtomicInteger readCount = new AtomicInteger(0);

  static String successReadResponseFormat
      = "{\"schemaVersion\":1,\"responseType\":\"Read\",\"status\":1001,\"httpStatus\":200,\"errorDescription\":\"OK\",\"elapsedTimeMs\":100,\"xMsRequestId\":\"e419c8fc-77cc-4d3f-8ed7-08fb83362355\",\"crc64\":55555,\"xMsClientRequestId\":\"CLIENT_PROV_CORR_ID_f5a469fc-c9ff-4cd6-9d27-98462972c6c1\",\"xMsVersion\":\"2019-12-12\",\"responseTimestamp\":\"Mon, 01 Feb 2021 12:57:36 GMT\",\"bytesRead\":%d,\"xMsContentCrc64\":1111}";

  static String throttledReadResponse
      = "{\"schemaVersion\":1,\"responseType\":\"Read\",\"status\":675543456,\"httpStatus\":503,\"errorDescription\":\"Request throttled\",\"elapsedTimeMs\":100,\"xMsRequestId\":\"a47dfca7-b717-405c-a08a-23dd87f7c895\",\"crc64\":55555,\"xMsClientRequestId\":\"CLIENT_PROV_CORR_ID_af215940-551e-48b7-afb9-4a9396b48d83\",\"xMsVersion\":\"2019-12-12\",\"responseTimestamp\":\"Mon, 01 Feb 2021 12:57:36 GMT\",\"bytesRead\":-1,\"xMsContentCrc64\":1111}";

  static String fileNotFoundReadResponse
      = "{\"schemaVersion\":1,\"responseType\":\"Read\",\"status\":137400330,\"httpStatus\":404,\"errorDescription\":\"FileNotFound\",\"elapsedTimeMs\":100,\"xMsRequestId\":\"a47dfca7-b717-405c-a08a-23dd87f7c895\",\"crc64\":55555,\"xMsClientRequestId\":\"CLIENT_PROV_CORR_ID_af215940-551e-48b7-afb9-4a9396b48d83\",\"xMsVersion\":\"2019-12-12\",\"responseTimestamp\":\"Mon, 01 Feb 2021 12:57:36 GMT\",\"bytesRead\":-1,\"xMsContentCrc64\":1111}";

  static String serverErrReadResponse
      = "{\"schemaVersion\":1,\"responseType\":\"Read\",\"status\":675543456,\"httpStatus\":500,\"errorDescription\":\"Internal server error\",\"elapsedTimeMs\":100,\"xMsRequestId\":\"a47dfca7-b717-405c-a08a-23dd87f7c895\",\"crc64\":55555,\"xMsClientRequestId\":\"CLIENT_PROV_CORR_ID_af215940-551e-48b7-afb9-4a9396b48d83\",\"xMsVersion\":\"2019-12-12\",\"responseTimestamp\":\"Mon, 01 Feb 2021 12:57:36 GMT\",\"bytesRead\":10,\"xMsContentCrc64\":1111}";

  static String fileNotFoundOpenResponse
      = "{\"schemaVersion\":1,\"responseType\":\"Open\",\"status\":137400330,\"httpStatus\":404,\"errorDescription\":\"FileNotFound\",\"elapsedTimeMs\":100,\"xMsRequestId\":\"a8034a68-235f-4868-a6b3-12e47bf2594a\",\"crc64\":55555,\"xMsClientRequestId\":\"CLIENT_PROV_CORR_ID_dc05a2aa-e145-4f33-b5d5-5424576c19f3\",\"xMsVersion\":\"2019-12-12\",\"responseTimestamp\":\"Mon, 01 Feb 2021 12:57:36 GMT\",\"handleKey\":\"b5d20440-ec95-4614-addb-26f9776a57ab\"}";

  static String serverErrOpenResponse
      = "{\"schemaVersion\":1,\"responseType\":\"Open\",\"status\":675543456,\"httpStatus\":500,\"errorDescription\":\"Internal server error\",\"elapsedTimeMs\":100,\"xMsRequestId\":\"a8034a68-235f-4868-a6b3-12e47bf2594a\",\"crc64\":55555,\"xMsClientRequestId\":\"CLIENT_PROV_CORR_ID_dc05a2aa-e145-4f33-b5d5-5424576c19f3\",\"xMsVersion\":\"2019-12-12\",\"responseTimestamp\":\"Mon, 01 Feb 2021 12:57:36 GMT\",\"handleKey\":\"b5d20440-ec95-4614-addb-26f9776a57ab\"}";

  static String successOpenResponse
      = "{\"schemaVersion\":1,\"responseType\":\"Open\",\"status\":1001,\"httpStatus\":200,\"errorDescription\":\"OK\",\"elapsedTimeMs\":100,\"xMsRequestId\":\"2201b43b-a471-43ce-85cb-9f0c5c5164cc\",\"crc64\":55555,\"xMsClientRequestId\":\"CLIENT_PROV_CORR_ID_9e40999c-a76a-4fc2-9d2b-983f6445fe51\",\"xMsVersion\":\"2019-12-12\",\"responseTimestamp\":\"Mon, 01 Feb 2021 12:57:36 GMT\",\"handleKey\":\"ff42514e-ffb9-428f-a830-1cc8601b4ec5\"}";

  static String serverErrCloseResponse
      = "{\"schemaVersion\":1,\"responseType\":\"Close\",\"status\":675543456,\"httpStatus\":500,\"errorDescription\":\"Internal server error\",\"elapsedTimeMs\":100,\"xMsRequestId\":\"fcb8502f-e1ac-4e91-b31c-a78cd2a98947\",\"crc64\":55555,\"xMsClientRequestId\":\"CLIENT_PROV_CORR_ID_e6c5b8c8-864e-4e1d-9592-b4f3a4b8c9ae\",\"xMsVersion\":\"2019-12-12\",\"responseTimestamp\":\"Mon, 01 Feb 2021 12:57:36 GMT\"}";

  static String successCloseResponse
      = "{\"schemaVersion\":1,\"responseType\":\"Close\",\"status\":1001,\"httpStatus\":200,\"errorDescription\":\"OK\",\"elapsedTimeMs\":100,\"xMsRequestId\":\"d2a810ba-db10-4735-bb4a-6ba090959a5d\",\"crc64\":55555,\"xMsClientRequestId\":\"CLIENT_PROV_CORR_ID_9ee436d3-9a88-4c3b-8c30-7d9e454392d2\",\"xMsVersion\":\"2019-12-12\",\"responseTimestamp\":\"Mon, 01 Feb 2021 12:57:36 GMT\"}";

  static String OPEN_OP = "open";
  static String READ_OP = "read";
  static String CLOSE_OP = "close";

  static HashMap<String, ByteBuffer> appendRegister
      = new HashMap<String, ByteBuffer>();

  int bufferOffset;

  public MockFastpathConnection() {
    super(true);
  }

  public static int getReadCounter() {
    return readCount.get();
  }

  public static void resetReadCount() {
    readCount.set(0);
  }

  public static void registerAppend(String requestPath,
      byte[] data,
      int offset,
      int length) {
    if (appendRegister.containsKey(requestPath)) {
      appendRegister.get(requestPath).put(data);
    } else {
      ByteBuffer bb = ByteBuffer.allocateDirect(1 * 1024 * 1024);
      bb.put(data, offset, length);
      bb.rewind();
      appendRegister.put(requestPath, bb);
    }
  }

  public static void registerAppend(int totalSize,
      String requestPath,
      byte[] data,
      int offset,
      int length) {
    if (appendRegister.containsKey(requestPath)) {
      LOG.debug("append to existing path: {} for size={} with offset={} len={}",
          requestPath, data.length, offset, length);
      appendRegister.get(requestPath).put(data, offset, length);
    } else {
      ByteBuffer bb = ByteBuffer.allocateDirect(totalSize);
      bb.put(data, offset, length);
      LOG.debug(
          "append registered for:{} for total size={} with new data len={}",
          requestPath, totalSize, length);
      appendRegister.put(requestPath, bb);
    }
  }

  public static void unregisterAppend(String requestPath) {
    if (appendRegister.containsKey(requestPath)) {
      appendRegister.remove(requestPath);
    }
  }

  public FastpathOpenResponse open(FastpathOpenRequestParams openParams) throws
      FastpathException {
    FastpathOpenResponse response;
    checkIfExceptionShouldbeThrown();
    String mockedResponse = getOpenResponse();
    this.getNativeApiCaller()
        .RegisterOpenResponse(openParams.getClientRequestId(),
            mockedResponse);
    try {
      response = super.open(openParams);
    } finally {
      this.getNativeApiCaller()
          .UnregisterOpenResponse(openParams.getClientRequestId(),
              mockedResponse);
    }

    return response;
  }

  private String getOpenResponse() {
    if (errStatus != 0) {
      return getErrorResponse(OPEN_OP, errStatus);
    }

    return successOpenResponse;
  }

  public FastpathReadResponse read(FastpathReadRequestParams readParams,
      byte[] buff) throws
      FastpathException {
    FastpathReadResponse response;
    String path = readParams.getUrl().getPath();
    path = path.substring(path.lastIndexOf("/") + 1);
    try {
      path = java.net.URLDecoder.decode(path, "UTF-8");
    } catch (java.io.UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    if (!appendRegister.containsKey(path)) {
      throw new FastpathException("no store file mock present for path:" + path + " - pos="
          + readParams.getStoreFilePosition() + ":bytesToRead="
          + readParams.getBytesToRead());
    } else {
      LOG.debug("PATH registered: {}", path);
    }
    ByteBuffer bb = clone(appendRegister.get(path));
    bb.rewind();
    checkIfExceptionShouldbeThrown();
    int len = Math.min((bb.capacity() - (int)readParams.getStoreFilePosition()),
        readParams.getBytesToRead());
    String mockedResponse = String.format(getReadResponse(), len);
    this.getNativeApiCaller()
        .RegisterReadResponse(readParams.getClientRequestId(),
            mockedResponse,
            readParams.getBytesToRead(),
            readParams.getStoreFilePosition(),
            bb);
    LOG.debug("register read response - done. read buff len={}", buff.length);
    try {
      response = super.read(readParams, buff);
      LOG.debug("read response - fetched");
      readCount.incrementAndGet();
    } finally {
      this.getNativeApiCaller()
          .UnregisterReadResponse(readParams.getClientRequestId(),
              mockedResponse);
      LOG.debug("unregister read response - done");
    }

    return response;
  }

  private String getReadResponse() {
    if (errStatus != 0) {
      return getErrorResponse(READ_OP, errStatus);
    }

    return successReadResponseFormat;
  }

  private static ByteBuffer clone(ByteBuffer original) {
    ByteBuffer clone = ByteBuffer.allocateDirect(original.capacity());
    original.rewind();
    clone.put(original);
    original.rewind();
    clone.flip();
    return clone;
  }

  public FastpathCloseResponse close(
      FastpathCloseRequestParams closeParams) throws
      FastpathException {

    FastpathCloseResponse response;
    checkIfExceptionShouldbeThrown();
    String mockedResponse = getCloseResponse();
    this.getNativeApiCaller()
        .RegisterCloseResponse(closeParams.getClientRequestId(),
            mockedResponse);
    try {
      response = super.close(closeParams);
    } finally {
      this.getNativeApiCaller()
          .UnregisterCloseResponse(closeParams.getClientRequestId(),
              mockedResponse);
    }
    return response;
  }

  private String getCloseResponse() {
    if (errStatus != 0) {
      return getErrorResponse(CLOSE_OP, errStatus);
    }

    return successCloseResponse;
  }

  protected FastpathDriver getNativeApiCaller() {
    return nativeApiCaller;
  }

  private void checkIfExceptionShouldbeThrown() throws FastpathException {
    if (mockRequestException) {
      throwFastpathRequestException();
    }

    if (mockConnectionException) {
      throwFastpathConnException();
    }
  }

  private String getErrorResponse(String opName, int errStatus) {
    if (opName.equals(OPEN_OP)) {
      switch(errStatus) {
      case 404: return fileNotFoundOpenResponse;
      case 500: return serverErrOpenResponse;
      default: return null;
      }
    }

    if (opName.equals(READ_OP)) {
      switch(errStatus) {
      case 503: return throttledReadResponse;
      case 404: return fileNotFoundReadResponse;
      case 500: return serverErrReadResponse;
      default: return null;
      }
    }

    if (opName.equals(CLOSE_OP)) {
      switch(errStatus) {
      case 500: return serverErrCloseResponse;
      default: return null;
      }
    }

    return null;
  }

  private void throwFastpathRequestException()
      throws FastpathRequestException {
    throw new FastpathRequestException("Test induced fastpath request exception");
  }

  private void throwFastpathConnException()
      throws FastpathConnectionException {
    throw new FastpathConnectionException("Test induced fastpath connection exception");
  }

  public void induceError(int httpStatus) {
    errStatus = httpStatus;
  }

  public void induceRequestException() {
    mockRequestException = true;
  }

  public void induceConnectionException() {
    mockConnectionException = true;
  }

  public void resetAllMockErrStates() {
    errStatus = 0;
    mockRequestException = false;
    mockConnectionException = false;
  }
}
