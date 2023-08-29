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

package org.apache.hadoop.yarn.client.util;

import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestFormattingCLIUtils {

  @Test
  public void testFormattingContent() throws URISyntaxException, IOException {
    String titleString = "4 queues were found";
    List<String> headerStrings = Arrays.asList("Queue Name", "Queue Path", "State", "Capacity",
        "Current Capacity", "Maximum Capacity", "Weight", "Maximum Parallel Apps");
    FormattingCLIUtils formattingCLIUtils = new FormattingCLIUtils(titleString)
        .addHeaders(headerStrings);
    DecimalFormat df = new DecimalFormat("#.00");

    formattingCLIUtils.addLine("queueA", "root.queueA",
        "RUNNING", df.format(0.4 * 100) + "%",
        df.format(0.5 * 100) + "%",
        df.format(0.8 * 100) + "%",
        df.format(-1),
        "10");
    formattingCLIUtils.addLine("queueB", "root.queueB",
        "RUNNING", df.format(0.4 * 100) + "%",
        df.format(0.5 * 100) + "%",
        df.format(0.8 * 100) + "%",
        df.format(-1),
        "10");
    formattingCLIUtils.addLine("queueC", "root.queueC",
        "RUNNING", df.format(0.4 * 100) + "%",
        df.format(0.5 * 100) + "%",
        df.format(0.8 * 100) + "%",
        df.format(-1),
        "10");
    formattingCLIUtils.addLine("queueD", "root.queueD",
        "RUNNING", df.format(0.4 * 100) + "%",
        df.format(0.5 * 100) + "%",
        df.format(0.8 * 100) + "%",
        df.format(-1),
        "10");
    StringBuilder resultStrBuilder = new StringBuilder();
    List<String> lines = Files.readAllLines(Paths
        .get(this.getClass().getResource("/FormattingResult").toURI()));
    for (String line : lines) {
      if (line != null && line.length() != 0 && !line.startsWith("#")) {
        resultStrBuilder.append(line + "\n");
      }
    }
    String expectStr = resultStrBuilder.toString();
    assertEquals(expectStr, formattingCLIUtils.render());
  }
}
