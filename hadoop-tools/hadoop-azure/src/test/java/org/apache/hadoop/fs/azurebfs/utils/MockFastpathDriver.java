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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.storage.fastpath.driver.FastpathDriver;
import com.azure.storage.fastpath.exceptions.FastpathException;
import com.azure.storage.fastpath.exceptions.FastpathRequestException;

public class MockFastpathDriver extends FastpathDriver {
  protected static final Logger LOG = LoggerFactory.getLogger(
      MockFastpathDriver.class);

  class ResponseRegisterFields {
    String mockResponse;
    long remotePosition;
    int bytesToRead;
    byte[] mockStoreBuffer;
  }

  static HashMap<String, ResponseRegisterFields> responseRegistry = new HashMap<String, ResponseRegisterFields>();

  public void RegisterOpenResponse(String clientRequestID,
      String jsonResponseString) {
    registerResponse(clientRequestID, jsonResponseString);
  }
  public void UnregisterOpenResponse(String clientRequestID,
      String jsonResponseString) {
    unregisterResponse(clientRequestID);
  }

  public void RegisterReadResponse(String clientRequestID,
      String jsonResponseString,
      int rdLength,
      long remoteFileOffset,
      ByteBuffer rdBuffer) {
    registerResponse(clientRequestID, jsonResponseString, rdLength,
        remoteFileOffset, rdBuffer);
  }

  public void UnregisterReadResponse(String clientRequestID,
      String jsonResponseString) {
    unregisterResponse(clientRequestID);
  }

  public void RegisterCloseResponse(String clientRequestID,
      String jsonResponseString) {
    registerResponse(clientRequestID, jsonResponseString);

  }
  public void UnregisterCloseResponse(String clientRequestID,
      String jsonResponseString) {
    unregisterResponse(clientRequestID);
  }

  public void registerResponse(String clientId, String response) {
    if (!responseRegistry.containsKey(clientId)) {
      ResponseRegisterFields fields = new ResponseRegisterFields();
      fields.bytesToRead = 0;
      fields.mockResponse = response;
      fields.mockStoreBuffer = null;
      fields.remotePosition = 0;
      responseRegistry.put(clientId, fields);
    }
  }

  public void registerResponse(String clientId,
      String response,
      int bytesToRead,
      long remoteFileOffset,
      ByteBuffer readBuffer) {
    if (!responseRegistry.containsKey(clientId)) {
      ResponseRegisterFields fields = new ResponseRegisterFields();
      fields.bytesToRead = bytesToRead;
      fields.mockResponse = response;
      fields.mockStoreBuffer = new byte[readBuffer.capacity()];
      readBuffer.rewind();
      readBuffer.position(0); readBuffer.get(fields.mockStoreBuffer);
      fields.remotePosition = remoteFileOffset;
      responseRegistry.put(clientId, fields);
    }
  }

  public void unregisterResponse(String clientId) {
    if (responseRegistry.containsKey(clientId)) {
      responseRegistry.remove(clientId);
    }
  }

  public String Open (int timeout,
      String clientRequestId,
      String serializedRequestParams) {
    return responseRegistry.get(clientRequestId).mockResponse;
  }

  @Override
  public String Read(final int timeout,
      final String clientRequestID,
      final String serializedRequestParams,
      final int rdBufOffset,
      final int readLength,
      final java.nio.ByteBuffer rdBuffer) throws FastpathException {
    byte[] tmpRdBBuffer = new byte[readLength];
    ReadByteArray(timeout, clientRequestID, serializedRequestParams,
        rdBufOffset, readLength, tmpRdBBuffer);
    rdBuffer.put(tmpRdBBuffer);
    return responseRegistry.get(clientRequestID).mockResponse;
  }

  @Override
  public String ReadByteArray(final int timeout,
      final String clientRequestId,
      final String serializedRequestParams,
      final int rdBufOffset,
      final int readLength,
      final byte[] rdBuffer) throws FastpathException {
    ResponseRegisterFields mockResponseRegister = responseRegistry.get(clientRequestId);
    org.junit.Assert.assertTrue(mockResponseRegister != null);
    int len = Math.min(
        (responseRegistry.get(clientRequestId).mockStoreBuffer.length
            - (int) responseRegistry.get(clientRequestId).remotePosition),
        readLength);
    if (len < 0) {
      throw new FastpathRequestException("invalid offset/read length ");
    } else if (len == 0) {
      LOG.debug("nothing left to read");
      return responseRegistry.get(clientRequestId).mockResponse;
    }

    String mockResponseFormat  = responseRegistry.get(clientRequestId).mockResponse;
    System.arraycopy(responseRegistry.get(clientRequestId).mockStoreBuffer,
        (int) responseRegistry.get(clientRequestId).remotePosition, rdBuffer,
        rdBufOffset, len);
    responseRegistry.get(clientRequestId).mockResponse = String.format(
        mockResponseFormat, len);
    return responseRegistry.get(clientRequestId).mockResponse;
  }

  public String Close (int timeout,
      String transactionId,
      String serializedRequestParams) {
    return responseRegistry.get(transactionId).mockResponse;
  }
}
