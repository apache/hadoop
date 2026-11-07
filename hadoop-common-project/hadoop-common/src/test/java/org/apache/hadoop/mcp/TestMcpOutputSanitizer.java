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

package org.apache.hadoop.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.junit.jupiter.api.Test;

public class TestMcpOutputSanitizer {

  private static final char ESC = 0x1B;

  private static String text(CallToolResult result) {
    return result.content().get(0).text();
  }

  @Test
  public void testControlCharactersRemoved() {
    McpOutputSanitizer sanitizer = new McpOutputSanitizer(0);

    // ESC is removed; tab, line feed and carriage return are kept.
    CallToolResult result = sanitizer.sanitize("tool",
        CallToolResult.text("a" + ESC + "[31mb\tc\nd\r\n"));

    assertFalse(result.isError());
    assertEquals("a[31mb\tc\nd\r\n", text(result));
  }

  @Test
  public void testOversizedOutputBecomesError() {
    McpOutputSanitizer sanitizer = new McpOutputSanitizer(5);

    CallToolResult result = sanitizer.sanitize("tool", CallToolResult.text("123456"));

    assertTrue(result.isError());
    assertEquals(McpOutputSanitizer.OUTPUT_TOO_LARGE_MESSAGE, text(result));
  }

  @Test
  public void testMissingResultBecomesError() {
    McpOutputSanitizer sanitizer = new McpOutputSanitizer(0);

    CallToolResult result = sanitizer.sanitize("tool", null);

    assertTrue(result.isError());
    assertEquals(McpOutputSanitizer.INVALID_RESULT_MESSAGE, text(result));
  }
}
