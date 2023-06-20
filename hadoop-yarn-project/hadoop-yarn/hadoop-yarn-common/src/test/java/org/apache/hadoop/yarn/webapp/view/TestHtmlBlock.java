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

package org.apache.hadoop.yarn.webapp.view;

import java.io.PrintWriter;

import com.google.inject.Injector;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.webapp.WebAppException;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

public class TestHtmlBlock {
  public static class TestBlock extends HtmlBlock {
    @Override
    public void render(Block html) {
      html.
        p("#testid").__("test note").__();
    }
  }

  public static class ShortBlock extends HtmlBlock {
    @Override
    public void render(Block html) {
      html.
        p().__("should throw");
    }
  }

  public static class ShortPage extends HtmlPage {
    @Override
    public void render(Page.HTML<__> html) {
      html.
        title("short test").
          __(ShortBlock.class);
    }
  }

  @Test
  void testUsual() {
    Injector injector = WebAppTests.testBlock(TestBlock.class);
    PrintWriter out = injector.getInstance(PrintWriter.class);

    verify(out).print(" id=\"testid\"");
    verify(out).print("test note");
  }

  @Test
  void testShortBlock() {
    assertThrows(WebAppException.class, () -> {
      WebAppTests.testBlock(ShortBlock.class);
    });
  }

  @Test
  void testShortPage() {
    assertThrows(WebAppException.class, () -> {
      WebAppTests.testPage(ShortPage.class);
    });
  }
}
