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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;

/**
 * Used for parsing metrics or configs to retrieve.
 */
@Private
@Unstable
public class TimelineParserForDataToRetrieve implements TimelineParser {
  private String expr;
  private final int exprLength;
  public TimelineParserForDataToRetrieve(String expression) {
    this.expr = expression;
    if (expression != null) {
      this.expr = expr.trim();
      exprLength = expr.length();
    } else {
      exprLength = 0;
    }
  }

  @Override
  public TimelineFilterList parse() throws TimelineParseException {
    if (expr == null || exprLength == 0) {
      return null;
    }
    TimelineCompareOp compareOp = null;
    int openingBracketIndex =
        expr.indexOf(TimelineParseConstants.OPENING_BRACKET_CHAR);
    if (expr.charAt(0) == TimelineParseConstants.NOT_CHAR) {
      if (openingBracketIndex == -1) {
        throw new TimelineParseException("Invalid config/metric to retrieve " +
            "expression");
      }
      if (openingBracketIndex != 1 &&
          expr.substring(1, openingBracketIndex + 1).trim().length() != 1) {
        throw new TimelineParseException("Invalid config/metric to retrieve " +
            "expression");
      }
      compareOp = TimelineCompareOp.NOT_EQUAL;
    } else if (openingBracketIndex <= 0) {
      compareOp = TimelineCompareOp.EQUAL;
    }
    char lastChar = expr.charAt(exprLength - 1);
    if (compareOp == TimelineCompareOp.NOT_EQUAL &&
        lastChar != TimelineParseConstants.CLOSING_BRACKET_CHAR) {
      throw new TimelineParseException("Invalid config/metric to retrieve " +
          "expression");
    }
    if (openingBracketIndex != -1 &&
        expr.charAt(exprLength - 1) ==
            TimelineParseConstants.CLOSING_BRACKET_CHAR) {
      expr = expr.substring(openingBracketIndex + 1, exprLength - 1).trim();
    }
    if (expr.isEmpty()) {
      return null;
    }
    Operator op =
        (compareOp == TimelineCompareOp.NOT_EQUAL) ? Operator.AND : Operator.OR;
    TimelineFilterList list = new TimelineFilterList(op);
    String[] splits = expr.split(TimelineParseConstants.COMMA_DELIMITER);
    for (String split : splits) {
      list.addFilter(new TimelinePrefixFilter(compareOp, split.trim()));
    }
    return list;
  }

  @Override
  public void close() {
  }
}
