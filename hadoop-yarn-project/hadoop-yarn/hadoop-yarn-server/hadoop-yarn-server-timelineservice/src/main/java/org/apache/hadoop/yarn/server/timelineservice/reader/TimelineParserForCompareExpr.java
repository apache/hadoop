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

import java.util.Deque;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;

/**
 * Abstract class for parsing compare expressions.
 * Compare expressions are of the form :
 * (&lt;key&gt; &lt;compareop&gt; &lt;value&gt;) &lt;op&gt; (&lt;key
 * &gt; &lt;compareop&gt; &lt;value&gt;)
 * compareop is used to compare value of a the specified key in the backend
 * storage. compareop can be :
 * 1. eq - Equals
 * 2. ne - Not equals (matches if key does not exist)
 * 3. ene - Exists and not equals (key must exist for match to occur)
 * 4. lt - Less than
 * 5. gt - Greater than
 * 6. le - Less than or equals
 * 7. ge - Greater than or equals
 * compareop's supported would depend on implementation. For instance, all
 * the above compareops' will be supported for metric filters but only eq,ne and
 * ene would be supported for KV filters like config/info filters.
 *
 * op is a logical operator and can be either AND or OR.
 *
 * The way values will be interpreted would also depend on implementation
 *
 * A typical compare expression would look as under:
 * ((key1 eq val1 OR key2 ne val2) AND (key5 gt val45))
 */
@Private
@Unstable
abstract class TimelineParserForCompareExpr implements TimelineParser {
  private enum ParseState {
    PARSING_KEY,
    PARSING_VALUE,
    PARSING_OP,
    PARSING_COMPAREOP
  }
  // Main expression.
  private final String expr;
  // Expression in lower case.
  private final String exprInLowerCase;
  private final String exprName;
  private int offset = 0;
  private int kvStartOffset = 0;
  private final int exprLength;
  private ParseState currentParseState = ParseState.PARSING_KEY;
  // Linked list implemented as a stack.
  private Deque<TimelineFilterList> filterListStack = new LinkedList<>();
  private TimelineFilter currentFilter = null;
  private TimelineFilterList filterList = null;
  public TimelineParserForCompareExpr(String expression, String name) {
    if (expression != null) {
      expr = expression.trim();
      exprLength = expr.length();
      exprInLowerCase = expr.toLowerCase();
    } else {
      expr = null;
      exprInLowerCase = null;
      exprLength = 0;
    }
    this.exprName = name;
  }

  protected TimelineFilter getCurrentFilter() {
    return currentFilter;
  }

  protected TimelineFilter getFilterList() {
    return filterList;
  }

  protected abstract TimelineFilter createFilter();

  protected abstract Object parseValue(String strValue)
      throws TimelineParseException;

  protected abstract void setCompareOpToCurrentFilter(
      TimelineCompareOp compareOp, boolean keyMustExistFlag)
      throws TimelineParseException;

  protected abstract void setValueToCurrentFilter(Object value);

  private void handleSpaceChar() throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_KEY ||
        currentParseState == ParseState.PARSING_VALUE) {
      if (kvStartOffset == offset) {
        kvStartOffset++;
        offset++;
        return;
      }
      String str = expr.substring(kvStartOffset, offset);
      if (currentParseState == ParseState.PARSING_KEY) {
        if (currentFilter == null) {
          currentFilter = createFilter();
        }
        ((TimelineCompareFilter)currentFilter).setKey(str);
        currentParseState = ParseState.PARSING_COMPAREOP;
      } else if (currentParseState == ParseState.PARSING_VALUE) {
        if (currentFilter != null) {
          setValueToCurrentFilter(parseValue(str));
        }
        currentParseState = ParseState.PARSING_OP;
      }
    }
    offset++;
  }

  private void handleOpeningBracketChar() throws TimelineParseException {
    if (currentParseState != ParseState.PARSING_KEY) {
      throw new TimelineParseException("Encountered unexpected opening " +
          "bracket while parsing " + exprName + ".");
    }
    offset++;
    kvStartOffset = offset;
    filterListStack.push(filterList);
    filterList = null;
  }

  private void handleClosingBracketChar() throws TimelineParseException {
    if (currentParseState != ParseState.PARSING_VALUE &&
        currentParseState != ParseState.PARSING_OP) {
      throw new TimelineParseException("Encountered unexpected closing " +
          "bracket while parsing " + exprName + ".");
    }
    if (!filterListStack.isEmpty()) {
      if (currentParseState == ParseState.PARSING_VALUE) {
        setValueToCurrentFilter(
            parseValue(expr.substring(kvStartOffset, offset)));
        currentParseState = ParseState.PARSING_OP;
      }
      if (currentFilter != null) {
        filterList.addFilter(currentFilter);
      }
      // As bracket is closing, pop the filter list from top of the stack and
      // combine it with current filter list.
      TimelineFilterList fList = filterListStack.pop();
      if (fList != null) {
        fList.addFilter(filterList);
        filterList = fList;
      }
      currentFilter = null;
      offset++;
      kvStartOffset = offset;
    } else {
      throw new TimelineParseException("Encountered unexpected closing " +
          "bracket while parsing " + exprName + ".");
    }
  }

  private void parseCompareOp() throws TimelineParseException {
    if (offset + 2 >= exprLength) {
      throw new TimelineParseException("Compare op cannot be parsed for " +
          exprName + ".");
    }
    TimelineCompareOp compareOp = null;
    boolean keyExistFlag = true;
    if (expr.charAt(offset + 2) == TimelineParseConstants.SPACE_CHAR) {
      if (exprInLowerCase.startsWith("eq", offset)) {
        compareOp = TimelineCompareOp.EQUAL;
      } else if (exprInLowerCase.startsWith("ne", offset)) {
        compareOp = TimelineCompareOp.NOT_EQUAL;
        keyExistFlag = false;
      } else if (exprInLowerCase.startsWith("lt", offset)) {
        compareOp = TimelineCompareOp.LESS_THAN;
      } else if (exprInLowerCase.startsWith("le", offset)) {
        compareOp = TimelineCompareOp.LESS_OR_EQUAL;
      } else if (exprInLowerCase.startsWith("gt", offset)) {
        compareOp = TimelineCompareOp.GREATER_THAN;
      } else if (exprInLowerCase.startsWith("ge", offset)) {
        compareOp = TimelineCompareOp.GREATER_OR_EQUAL;
      }
      offset = offset + 3;
    } else if (exprInLowerCase.startsWith("ene ", offset)) {
      // Not equal but key should be present.
      compareOp = TimelineCompareOp.NOT_EQUAL;
      offset = offset + 4;
    }
    if (compareOp == null) {
      throw new TimelineParseException("Compare op cannot be parsed for " +
          exprName + ".");
    }
    setCompareOpToCurrentFilter(compareOp, keyExistFlag);
    kvStartOffset = offset;
    currentParseState = ParseState.PARSING_VALUE;
  }

  private void parseOp(boolean closingBracket) throws TimelineParseException {
    Operator operator = null;
    if (exprInLowerCase.startsWith("or ", offset)) {
      operator = Operator.OR;
      offset = offset + 3;
    } else if (exprInLowerCase.startsWith("and ", offset)) {
      operator = Operator.AND;
      offset = offset + 4;
    }
    if (operator == null) {
      throw new TimelineParseException("Operator cannot be parsed for " +
          exprName + ".");
    }
    if (filterList == null) {
      filterList = new TimelineFilterList(operator);
    }
    if (currentFilter != null) {
      filterList.addFilter(currentFilter);
    }
    if (closingBracket || filterList.getOperator() != operator) {
      filterList = new TimelineFilterList(operator, filterList);
    }
    currentFilter = null;
    kvStartOffset = offset;
    currentParseState = ParseState.PARSING_KEY;
  }

  @Override
  public TimelineFilterList parse() throws TimelineParseException {
    if (expr == null || exprLength == 0) {
      return null;
    }
    boolean closingBracket = false;
    while (offset < exprLength) {
      char offsetChar = expr.charAt(offset);
      switch(offsetChar) {
      case TimelineParseConstants.SPACE_CHAR:
        handleSpaceChar();
        break;
      case TimelineParseConstants.OPENING_BRACKET_CHAR:
        handleOpeningBracketChar();
        break;
      case TimelineParseConstants.CLOSING_BRACKET_CHAR:
        handleClosingBracketChar();
        closingBracket = true;
        break;
      default: // other characters.
        // Parse based on state.
        if (currentParseState == ParseState.PARSING_COMPAREOP) {
          parseCompareOp();
        } else if (currentParseState == ParseState.PARSING_OP) {
          parseOp(closingBracket);
          closingBracket = false;
        } else {
          // Might be a key or value. Move ahead.
          offset++;
        }
        break;
      }
    }
    if (!filterListStack.isEmpty()) {
      filterListStack.clear();
      throw new TimelineParseException("Encountered improper brackets while " +
          "parsing " + exprName + ".");
    }
    if (currentParseState == ParseState.PARSING_VALUE) {
      setValueToCurrentFilter(
          parseValue(expr.substring(kvStartOffset, offset)));
    }
    if (filterList == null || filterList.getFilterList().isEmpty()) {
      if (currentFilter == null) {
        throw new TimelineParseException(
            "Invalid expression provided for " + exprName);
      } else {
        filterList = new TimelineFilterList(currentFilter);
      }
    } else if (currentFilter != null) {
      filterList.addFilter(currentFilter);
    }
    return filterList;
  }

  @Override
  public void close() {
    if (filterListStack != null) {
      filterListStack.clear();
    }
    filterList = null;
    currentFilter = null;
  }
}