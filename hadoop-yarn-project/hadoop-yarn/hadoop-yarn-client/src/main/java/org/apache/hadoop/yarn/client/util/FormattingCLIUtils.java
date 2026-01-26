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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The main core class that generates the ASCII TABLE.
 */
public final class FormattingCLIUtils {
  /** Table title. */
  private String title;
  /** Last processed row type. */
  private TableRowType lastTableRowType;
  /** StringBuilder object used to concatenate strings. */
  private StringBuilder join;
  /** An ordered Map that holds each row of data. */
  private List<TableRow> tableRows;
  /** Maps the maximum length of each column. */
  private Map<Integer, Integer> maxColMap;

  /**
   * Contains the title constructor.
   * @param title titleName
   */
  public FormattingCLIUtils(String title) {
    this.init();
    this.title = title;
  }

  /**
   * Initialize the data.
   */
  private void init() {
    this.join = new StringBuilder();
    this.tableRows = new ArrayList<>();
    this.maxColMap = new HashMap<>();
  }

  /**
   * Adds elements from the collection to the header data in the table.
   * @param headers Header data
   * @return FormattingCLIUtils object
   */
  public FormattingCLIUtils addHeaders(List<?> headers) {
    return this.appendRows(TableRowType.HEADER, headers.toArray());
  }

  /**
   * Adds a row of normal data to the table.
   * @param objects Common row data
   * @return FormattingCLIUtils object
   */
  public FormattingCLIUtils addLine(Object... objects) {
    return this.appendRows(TableRowType.LINE, objects);
  }

  /**
   * Adds the middle row of data to the table.
   * @param tableRowType TableRowType
   * @param objects Table row data
   * @return FormattingCLIUtils object
   */
  private FormattingCLIUtils appendRows(TableRowType tableRowType, Object... objects) {
    if (objects != null && objects.length > 0) {
      int len = objects.length;
      if (this.maxColMap.size() > len) {
        throw new IllegalArgumentException("The number of columns that inserted a row " +
            "of data into the table is different from the number of previous columns, check!");
      }
      List<String> lines = new ArrayList<>();
      for (int i = 0; i < len; i++) {
        Object o = objects[i];
        String value = o == null ? "null" : o.toString();
        lines.add(value);
        Integer maxColSize = this.maxColMap.get(i);
        if (maxColSize == null) {
          this.maxColMap.put(i, value.length());
          continue;
        }
        if (value.length() > maxColSize) {
          this.maxColMap.put(i, value.length());
        }
      }
      this.tableRows.add(new TableRow(tableRowType, lines));
    }
    return this;
  }

  /**
   * Builds the string for the row of the table title.
   */
  private void buildTitle() {
    if (this.title != null) {
      int maxTitleSize = 0;
      for (Integer maxColSize : this.maxColMap.values()) {
        maxTitleSize += maxColSize;
      }
      maxTitleSize += 3 * (this.maxColMap.size() - 1);
      if (this.title.length() > maxTitleSize) {
        this.title = this.title.substring(0, maxTitleSize);
      }
      this.join.append("+");
      for (int i = 0; i < maxTitleSize + 2; i++) {
        this.join.append("-");
      }
      this.join.append("+\n")
          .append("|")
          .append(StrUtils.center(this.title, maxTitleSize + 2, ' '))
          .append("|\n");
      this.lastTableRowType = TableRowType.TITLE;
    }
  }

  /**
   * Build the table, first build the title, and then walk through each row of data to build.
   */
  private void buildTable() {
    this.buildTitle();
    for (int i = 0, len = this.tableRows.size(); i < len; i++) {
      List<String> data = this.tableRows.get(i).data;
      switch (this.tableRows.get(i).tableRowType) {
      case HEADER:
        if (this.lastTableRowType != TableRowType.HEADER) {
          this.buildRowBorder(data);
        }
        this.buildRowLine(data);
        this.buildRowBorder(data);
        break;
      case LINE:
        this.buildRowLine(data);
        if (i == len - 1) {
          this.buildRowBorder(data);
        }
        break;
      default:
        break;
      }
    }
  }

  /**
   * Method to build a border row.
   * @param data dataLine
   */
  private void buildRowBorder(List<String> data) {
    this.join.append("+");
    for (int i = 0, len = data.size(); i < len; i++) {
      for (int j = 0; j < this.maxColMap.get(i) + 2; j++) {
        this.join.append("-");
      }
      this.join.append("+");
    }
    this.join.append("\n");
  }

  /**
   * A way to build rows of data.
   * @param data dataLine
   */
  private void buildRowLine(List<String> data) {
    this.join.append("|");
    for (int i = 0, len = data.size(); i < len; i++) {
      this.join.append(StrUtils.center(data.get(i), this.maxColMap.get(i) + 2, ' '))
          .append("|");
    }
    this.join.append("\n");
  }

  /**
   * Rendering is born as a result.
   * @return ASCII string of Table
   */
  public String render() {
    this.buildTable();
    return this.join.toString();
  }

  /**
   * The type of each table row and the entity class of the data.
   */
  private static class TableRow {
    private TableRowType tableRowType;
    private List<String> data;
    TableRow(TableRowType tableRowType, List<String> data) {
      this.tableRowType = tableRowType;
      this.data = data;
    }
  }

  /**
   * An enumeration class that distinguishes between table headers and normal table data.
   */
  private enum TableRowType {
    TITLE, HEADER, LINE
  }

  /**
   * String utility class.
   */
  private static final class StrUtils {
    /**
     * Puts a string in the middle of a given size.
     * @param str Character string
     * @param size Total size
     * @param padChar Fill character
     * @return String result
     */
    private static String center(String str, int size, char padChar) {
      if (str != null && size > 0) {
        int strLen = str.length();
        int pads = size - strLen;
        if (pads > 0) {
          str = leftPad(str, strLen + pads / 2, padChar);
          str = rightPad(str, size, padChar);
        }
      }
      return str;
    }

    /**
     * Left-fill the given string and size.
     * @param str String
     * @param size totalSize
     * @param padChar Fill character
     * @return String result
     */
    private static String leftPad(final String str, int size, char padChar) {
      int pads = size - str.length();
      return pads <= 0 ? str : repeat(padChar, pads).concat(str);
    }

    /**
     * Right-fill the given string and size.
     * @param str String
     * @param size totalSize
     * @param padChar Fill character
     * @return String result
     */
    private static String rightPad(final String str, int size, char padChar) {
      int pads = size - str.length();
      return pads <= 0 ? str : str.concat(repeat(padChar, pads));
    }

    /**
     * Re-fill characters as strings.
     * @param ch String
     * @param repeat Number of repeats
     * @return String
     */
    private static String repeat(char ch, int repeat) {
      char[] buf = new char[repeat];
      for (int i = repeat - 1; i >= 0; i--) {
        buf[i] = ch;
      }
      return new String(buf);
    }
  }
}
