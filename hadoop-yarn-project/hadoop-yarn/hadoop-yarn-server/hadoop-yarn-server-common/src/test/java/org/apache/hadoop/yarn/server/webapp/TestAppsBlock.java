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

package org.apache.hadoop.yarn.server.webapp;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.Range;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetApplicationsRequestPBImpl;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlBlockForTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestAppsBlock {

  public static Stream<Arguments> startTimeRanges() {
    return Stream.of(
        Arguments.of(null, null, null),
        Arguments.of("0", Long.toString(Long.MAX_VALUE), null),
        Arguments.of("10", null, Range.between(10L, Long.MAX_VALUE)),
        Arguments.of(null, "20", Range.between(0L, 20L)),
        Arguments.of("10", "20", Range.between(10L, 20L)),
        Arguments.of("20", "20", Range.between(20L, 20L)),
        Arguments.of("0", "0", Range.between(0L, 0L)));
  }

  @ParameterizedTest
  @MethodSource("startTimeRanges")
  public void testStartTimeRange(String begin, String end,
      Range<Long> expectedRange) throws Exception {
    assertEquals(expectedRange, fetchApplications(begin, end).getStartRange());
  }

  public static Stream<Arguments> invalidStartTimeRanges() {
    return Stream.of(
        Arguments.of("-1", null),
        Arguments.of(null, "-1"),
        Arguments.of("20", "10"));
  }

  @ParameterizedTest
  @MethodSource("invalidStartTimeRanges")
  public void testInvalidStartTimeRange(String begin, String end) {
    assertThrows(BadRequestException.class,
        () -> fetchApplications(begin, end));
  }

  private static GetApplicationsRequest fetchApplications(String begin,
      String end) throws Exception {
    Map<String, String> params = new HashMap<>();
    if (begin != null) {
      params.put(YarnWebParams.APP_START_TIME_BEGIN, begin);
    }
    if (end != null) {
      params.put(YarnWebParams.APP_START_TIME_END, end);
    }
    ApplicationBaseProtocol protocol = mock(ApplicationBaseProtocol.class);
    when(protocol.getApplications(any(GetApplicationsRequest.class)))
        .thenReturn(GetApplicationsResponse.newInstance(Collections.emptyList()));
    AppsBlock appBlock = new AppsBlock(protocol, null) {
      @Override
      public String $(String key, String defaultValue) {
        return params.getOrDefault(key, defaultValue);
      }

      @Override
      protected UserGroupInformation getCallerUGI() {
        return null;
      }
    };
    appBlock.fetchData();

    ArgumentCaptor<GetApplicationsRequest> requestCaptor =
        ArgumentCaptor.forClass(GetApplicationsRequest.class);
    verify(protocol).getApplications(requestCaptor.capture());
    GetApplicationsRequestPBImpl request =
        (GetApplicationsRequestPBImpl) requestCaptor.getValue();
    return new GetApplicationsRequestPBImpl(request.getProto());
  }

  /**
   * Test invalid application state.Exception should be thrown if application
   * state is not valid.
   */
  @Test
  public void testInvalidAppState() {
    assertThrows(IllegalArgumentException.class, () -> {
      AppsBlock appBlock = new AppsBlock(null, null) {
        // override this so that apps block can fetch app state.
        @Override
        public Map<String, String> moreParams() {
          Map<String, String> map = new HashMap<>();
          map.put(YarnWebParams.APP_STATE, "ACCEPTEDPING");
          return map;
        }

        @Override
        protected void renderData(Block html) {
        }
      };

      // set up the test block to render AppsBlock
      OutputStream outputStream = new ByteArrayOutputStream();
      HtmlBlock.Block block = createBlockToCreateTo(outputStream);

      // If application state is invalid it should throw exception
      // instead of catching it.
      appBlock.render(block);
    });
  }

  private static HtmlBlock.Block createBlockToCreateTo(
      OutputStream outputStream) {
    PrintWriter printWriter = new PrintWriter(outputStream);
    HtmlBlock html = new HtmlBlockForTest();
    return new BlockForTest(html, printWriter, 10, false) {
      @Override
      protected void subView(Class<? extends SubView> cls) {
      }
    };
  };

}
