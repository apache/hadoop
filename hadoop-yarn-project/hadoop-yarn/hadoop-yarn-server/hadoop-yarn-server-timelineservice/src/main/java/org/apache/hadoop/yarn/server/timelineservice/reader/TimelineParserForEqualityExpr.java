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
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;

/**
 * Abstract class for parsing equality expressions. This means the values in
 * expression would either be equal or not equal.
 * Equality expressions are of the form :
 * (&lt;value&gt;,&lt;value&gt;,&lt;value&gt;) &lt;op&gt; !(&lt;value&gt;,
 * &lt;value&gt;)
 *
 * Here, "!" means all the values should not exist/should not be equal.
 * If not specified, they should exist/be equal.
 *
 * op is a logical operator and can be either AND or OR.
 *
 * The way values will be interpreted would also depend on implementation.
 *
 * For instance for event filters this expression may look like,
 * (event1,event2) AND !(event3,event4)
 * This means for an entity to match, event1 and event2 should exist. But event3
 * and event4 should not exist.
 */
@Private
@Unstable
abstract class TimelineParserForEqualityExpr implements TimelineParser {
  private enum ParseState {
    PARSING_VALUE,
    PARSING_OP,
    PARSING_COMPAREOP
  }
  private final String expr;
  // Expression in lower case.
  private final String exprInLowerCase;
  // Expression name.
  private final String exprName;
  // Expression offset.
  private int offset = 0;
  // Offset used to parse values in the expression.
  private int startOffset = 0;
  private final int exprLength;
  private ParseState currentParseState = ParseState.PARSING_COMPAREOP;
  private TimelineCompareOp currentCompareOp = null;
  // Used to store filter lists which can then be combined as brackets are
  // closed.
  private Deque<TimelineFilterList> filterListStack = new LinkedList<>();
  private TimelineFilter currentFilter = null;
  private TimelineFilterList filterList = null;
  // Delimiter used to separate values.
  private final char delimiter;
  public TimelineParserForEqualityExpr(String expression, String name,
      char delim) {
    if (expression != null) {
      expr = expression.trim();
      exprLength = expr.length();
      exprInLowerCase = expr.toLowerCase();
    } else {
      exprLength = 0;
      expr = null;
      exprInLowerCase = null;
    }
    exprName = name;
    delimiter = delim;
  }

  protected TimelineFilter getCurrentFilter() {
    return currentFilter;
  }

  protected TimelineFilter getFilterList() {
    return filterList;
  }

  /**
   * Creates filter as per implementation.
   *
   * @return a {@link TimelineFilter} implementation.
   */
  protected abstract TimelineFilter createFilter();

  /**
   * Sets compare op to the current filter as per filter implementation.
   *
   * @param compareOp compare op to be set.
   * @throws Exception if any problem occurs.
   */
  protected abstract void setCompareOpToCurrentFilter(
      TimelineCompareOp compareOp) throws TimelineParseException;

  /**
   * Sets value to the current filter as per filter implementation.
   *
   * @param value value to be set.
   * @throws Exception if any problem occurs.
   */
  protected abstract void setValueToCurrentFilter(String value)
      throws TimelineParseException;

  private void createAndSetFilter(boolean checkIfNull)
      throws TimelineParseException {
    if (!checkIfNull || currentFilter == null) {
      currentFilter = createFilter();
      setCompareOpToCurrentFilter(currentCompareOp);
    }
    setValueToCurrentFilter(expr.substring(startOffset, offset).trim());
  }

  private void handleSpaceChar() throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_VALUE) {
      if (startOffset == offset) {
        startOffset++;
      } else {
        createAndSetFilter(true);
        currentParseState = ParseState.PARSING_OP;
      }
    }
    offset++;
  }

  private void handleDelimiter() throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_OP ||
        currentParseState == ParseState.PARSING_VALUE) {
      if (currentParseState == ParseState.PARSING_VALUE) {
        createAndSetFilter(false);
      }
      if (filterList == null) {
        filterList = new TimelineFilterList();
      }
      // Add parsed filter into filterlist and make it null to move on to next
      // filter.
      filterList.addFilter(currentFilter);
      currentFilter = null;
      offset++;
      startOffset = offset;
      currentParseState = ParseState.PARSING_VALUE;
    } else {
      throw new TimelineParseException("Invalid " + exprName + "expression.");
    }
  }

  private void handleOpeningBracketChar(boolean encounteredNot)
      throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_COMPAREOP ||
        currentParseState == ParseState.PARSING_VALUE) {
      offset++;
      startOffset = offset;
      filterListStack.push(filterList);
      filterList = null;
      if (currentFilter == null) {
        currentFilter = createFilter();
      }
      currentCompareOp = encounteredNot ?
          TimelineCompareOp.NOT_EQUAL : TimelineCompareOp.EQUAL;
      setCompareOpToCurrentFilter(currentCompareOp);
      currentParseState = ParseState.PARSING_VALUE;
    } else {
      throw new TimelineParseException("Encountered unexpected opening " +
          "bracket while parsing " + exprName + ".");
    }
  }

  private void handleNotChar() throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_COMPAREOP ||
        currentParseState == ParseState.PARSING_VALUE) {
      offset++;
      while (offset < exprLength &&
          expr.charAt(offset) == TimelineParseConstants.SPACE_CHAR) {
        offset++;
      }
      if (offset == exprLength) {
        throw new TimelineParseException("Invalid " + exprName + "expression");
      }
      if (expr.charAt(offset) == TimelineParseConstants.OPENING_BRACKET_CHAR) {
        handleOpeningBracketChar(true);
      } else {
        throw new TimelineParseException("Invalid " + exprName + "expression");
      }
    } else {
      throw new TimelineParseException("Encountered unexpected not(!) char " +
         "while parsing " + exprName + ".");
    }
  }

  private void handleClosingBracketChar() throws TimelineParseException {
    if (currentParseState != ParseState.PARSING_VALUE &&
        currentParseState != ParseState.PARSING_OP) {
      throw new TimelineParseException("Encountered unexpected closing " +
          "bracket while parsing " + exprName + ".");
    }
    if (!filterListStack.isEmpty()) {
      if (currentParseState == ParseState.PARSING_VALUE) {
        if (startOffset != offset) {
          createAndSetFilter(true);
          currentParseState = ParseState.PARSING_OP;
        }
      }
      if (filterList == null) {
        filterList = new TimelineFilterList();
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
      startOffset = offset;
    } else {
      throw new TimelineParseException("Encountered unexpected closing " +
          "bracket while parsing " + exprName + ".");
    }
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
    startOffset = offset;
    currentParseState = ParseState.PARSING_COMPAREOP;
  }

  private void parseCompareOp() throws TimelineParseException {
    if (currentFilter == null) {
      currentFilter = createFilter();
    }
    currentCompareOp = TimelineCompareOp.EQUAL;
    setCompareOpToCurrentFilter(currentCompareOp);
    currentParseState = ParseState.PARSING_VALUE;
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
      case TimelineParseConstants.NOT_CHAR:
        handleNotChar();
        break;
      case TimelineParseConstants.SPACE_CHAR:
        handleSpaceChar();
        break;
      case TimelineParseConstants.OPENING_BRACKET_CHAR:
        handleOpeningBracketChar(false);
        break;
      case TimelineParseConstants.CLOSING_BRACKET_CHAR:
        handleClosingBracketChar();
        closingBracket = true;
        break;
      default: // other characters.
        if (offsetChar == delimiter) {
          handleDelimiter();
        } else if (currentParseState == ParseState.PARSING_COMPAREOP) {
          parseCompareOp();
        } else if (currentParseState == ParseState.PARSING_OP) {
          parseOp(closingBracket);
          closingBracket = false;
        } else {
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
      if (startOffset != offset) {
        createAndSetFilter(true);
      }
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
    currentFilter = null;
    filterList = null;
  }
}