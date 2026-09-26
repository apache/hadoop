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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlBlockForTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestAppsBlock {

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

  /**
   * The tracking URL is registered by the application master, so it must be
   * escaped before it is written into the apps table script block.
   */
  @Test
  public void testTrackingUrlIsEscaped() {
    String trackingUrl = "http://tracking/\"</script><script>alert(1)</script>";

    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getApplicationId())
        .thenReturn(ApplicationId.newInstance(0, 1));
    when(report.getUser()).thenReturn("user");
    when(report.getQueue()).thenReturn("default");
    when(report.getName()).thenReturn("app");
    when(report.getApplicationType()).thenReturn("type");
    when(report.getYarnApplicationState())
        .thenReturn(YarnApplicationState.RUNNING);
    when(report.getFinalApplicationStatus())
        .thenReturn(FinalApplicationStatus.UNDEFINED);
    when(report.getTrackingUrl()).thenReturn(trackingUrl);

    AppsBlock appsBlock = new AppsBlock(null, null) {
      @Override
      public String url(String... parts) {
        return "/app";
      }
    };
    appsBlock.reqAppStates = EnumSet.noneOf(YarnApplicationState.class);
    appsBlock.appReports = Collections.singletonList(report);

    OutputStream outputStream = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(outputStream);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false) {
      @Override
      protected void subView(Class<? extends SubView> cls) {
      }
    };
    appsBlock.renderData(block);
    printWriter.flush();

    String rendered = outputStream.toString();
    assertFalse(rendered.contains("<script>alert(1)</script>"),
        "tracking url was not escaped: " + rendered);
    assertTrue(rendered.contains("&lt;\\/script&gt;&lt;script&gt;"),
        "tracking url was not escaped: " + rendered);
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
