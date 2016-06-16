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
 *
 */

package org.apache.hadoop.fs.common;

import com.squareup.okhttp.mockwebserver.Dispatcher;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.hadoop.fs.adl.TestADLResponseData;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Supporting class for mock test to validate Adls read operation using
 * BufferManager.java and BatchByteArrayInputStream implementation.
 */
public class TestDataForRead {

  private byte[] actualData;
  private ArrayList<ExpectedResponse> responses;
  private Dispatcher dispatcher;
  private int intensityOfTest;
  private boolean checkOfNoOfCalls;
  private int expectedNoNetworkCall;

  public TestDataForRead(final byte[] actualData, int expectedNoNetworkCall,
      int intensityOfTest, boolean checkOfNoOfCalls) {
    this.checkOfNoOfCalls = checkOfNoOfCalls;
    this.actualData = actualData;
    responses = new ArrayList<ExpectedResponse>();
    this.expectedNoNetworkCall = expectedNoNetworkCall;
    this.intensityOfTest = intensityOfTest;

    dispatcher = new Dispatcher() {
      @Override
      public MockResponse dispatch(RecordedRequest recordedRequest)
          throws InterruptedException {
        if (recordedRequest.getPath().equals("/refresh")) {
          return AdlMockWebServer.getTokenResponse();
        }

        if (recordedRequest.getRequestLine().contains("op=GETFILESTATUS")) {
          return new MockResponse().setResponseCode(200).setBody(
              TestADLResponseData
                  .getGetFileStatusJSONResponse(actualData.length));
        }

        if (recordedRequest.getRequestLine().contains("op=OPEN")) {
          String request = recordedRequest.getRequestLine();
          int offset = 0;
          int byteCount = 0;

          Pattern pattern = Pattern.compile("offset=([0-9]+)");
          Matcher matcher = pattern.matcher(request);
          if (matcher.find()) {
            System.out.println(matcher.group(1));
            offset = Integer.parseInt(matcher.group(1));
          }

          pattern = Pattern.compile("length=([0-9]+)");
          matcher = pattern.matcher(request);
          if (matcher.find()) {
            System.out.println(matcher.group(1));
            byteCount = Integer.parseInt(matcher.group(1));
          }

          Buffer buf = new Buffer();
          buf.write(actualData, offset, byteCount);
          return new MockResponse().setResponseCode(200)
              .setChunkedBody(buf, 4 * 1024 * 1024);
        }

        return new MockResponse().setBody("NOT SUPPORTED").setResponseCode(501);
      }
    };
  }

  public boolean isCheckOfNoOfCalls() {
    return checkOfNoOfCalls;
  }

  public int getExpectedNoNetworkCall() {
    return expectedNoNetworkCall;
  }

  public int getIntensityOfTest() {
    return intensityOfTest;
  }

  public byte[] getActualData() {
    return actualData;
  }

  public ArrayList<ExpectedResponse> getResponses() {
    return responses;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }
}
