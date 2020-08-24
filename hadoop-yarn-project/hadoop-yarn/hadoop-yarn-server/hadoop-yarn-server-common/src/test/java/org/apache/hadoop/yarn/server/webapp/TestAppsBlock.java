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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlBlockForTest;
import org.junit.Test;

public class TestAppsBlock {

  /**
   * Test invalid application state.Exception should be thrown if application
   * state is not valid.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAppState() {
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
