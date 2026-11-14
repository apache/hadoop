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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mcp.McpSchema.CallToolResult;
import org.apache.hadoop.mcp.McpSchema.TextContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sanitizes tool results before they are sent to the client:
 * <ul>
 *   <li>removes control characters other than tab, line feed and carriage return, so
 *       output can't carry terminal escape sequences;</li>
 *   <li>replaces a result whose text exceeds the configured length with an error;</li>
 *   <li>replaces a missing result, or one with missing text, with an error.</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class McpOutputSanitizer {

  private static final Logger LOG = LoggerFactory.getLogger(McpOutputSanitizer.class);

  static final String INVALID_RESULT_MESSAGE = "Tool returned an invalid result";
  static final String OUTPUT_TOO_LARGE_MESSAGE = "Tool output exceeds the maximum size";

  /** C0 and C1 control characters and DEL, except tab, line feed and carriage return. */
  private static final Pattern CONTROL_CHARACTERS =
      Pattern.compile("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F-\\x9F]");

  private final int maxOutputChars;

  /** @param maxOutputChars maximum total text length of a result; 0 or less disables it */
  McpOutputSanitizer(int maxOutputChars) {
    this.maxOutputChars = maxOutputChars;
  }

  CallToolResult sanitize(String toolName, CallToolResult result) {
    if (result == null || result.content() == null) {
      LOG.warn("MCP tool {} returned no result", toolName);
      return CallToolResult.error(INVALID_RESULT_MESSAGE);
    }
    List<TextContent> content = new ArrayList<>(result.content().size());
    long length = 0;
    for (TextContent item : result.content()) {
      if (item == null || item.text() == null) {
        LOG.warn("MCP tool {} returned content without text", toolName);
        return CallToolResult.error(INVALID_RESULT_MESSAGE);
      }
      String text = CONTROL_CHARACTERS.matcher(item.text()).replaceAll("");
      length += text.length();
      content.add(new TextContent(text));
    }
    if (maxOutputChars > 0 && length > maxOutputChars) {
      LOG.warn("MCP tool {} returned {} characters, more than the limit of {}",
          toolName, length, maxOutputChars);
      return CallToolResult.error(OUTPUT_TOO_LARGE_MESSAGE);
    }
    return CallToolResult.of(content, result.isError());
  }
}
