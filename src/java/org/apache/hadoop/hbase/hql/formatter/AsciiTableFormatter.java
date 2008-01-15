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
package org.apache.hadoop.hbase.hql.formatter;

import java.io.IOException;
import java.io.Writer;

import org.apache.hadoop.hbase.hql.TableFormatter;


/**
 * Formatter that outputs data inside an ASCII table.
 * If only a single cell result, then no formatting is done.  Presumption is
 * that client manages serial access outputting tables. Does not close passed
 * {@link Writer}.
 */
public class AsciiTableFormatter implements TableFormatter {
  private static final String COLUMN_DELIMITER = "| ";
  private static final String COLUMN_CLOSER = "|";
  private static final int DEFAULT_COLUMN_WIDTH = 26;
  // Width is a line of content + delimiter
  private int columnWidth = DEFAULT_COLUMN_WIDTH;
  // Amount of width to use for a line of content.
  private int columnContentWidth =
    DEFAULT_COLUMN_WIDTH - COLUMN_DELIMITER.length();
  // COLUMN_LINE is put at head and foot of a column and per column, is drawn
  // as row delimiter
  private String columnHorizLine;
  private final String COLUMN_HORIZ_LINE_CLOSER = "+";
  // Used padding content to fill column
  private final String PADDING_CHAR = " ";
  // True if we are to output no formatting.
  private boolean noFormatting = false;
  private final Writer out;
  private final String LINE_SEPARATOR = System.getProperty("line.separator");

  // Not instantiable
  @SuppressWarnings("unused")
  private AsciiTableFormatter() {
    this(null);
  }
  
  public AsciiTableFormatter(final Writer o) {
    this.out = o;
  }
  
  public Writer getOut() {
    return this.out;
  }
  
  /**
   * @param titles List of titles.  Pass null if no formatting (i.e.
   * no header, no footer, etc.
   * @throws IOException 
   */
  public void header(String[] titles) throws IOException {
    if (titles == null) {
      // print nothing.
      setNoFormatting(true);
      return;
    }
    // Calculate width of columns.
    this.columnWidth = titles.length == 1? 3 * DEFAULT_COLUMN_WIDTH:
      titles.length == 2? 39: DEFAULT_COLUMN_WIDTH;
    this.columnContentWidth = this.columnWidth - COLUMN_DELIMITER.length();
    // Create the horizontal line to draw across the top of each column.
    this.columnHorizLine = calculateColumnHorizLine(this.columnWidth);
    // Print out a column topper per column.
    printRowDelimiter(titles.length);
    row(titles);
  }

  public void row(String [] cells) throws IOException {
    if (isNoFormatting()) {
      getOut().write(cells[0]);
      getOut().flush();
      return;
    }
    // Ok.  Output cells a line at a time w/ delimiters between cells.
    int [] indexes = new int[cells.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = 0;
    }
    int allFinished = 0;
    while (allFinished < indexes.length) {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < cells.length; i++) {
        sb.append(COLUMN_DELIMITER);
        int offset = indexes[i];
        if (offset + this.columnContentWidth >= cells[i].length()) {
          String substr = cells[i].substring(offset);
          if (substr.length() > 0) {
            // This column is finished
            allFinished++;
            sb.append(substr);
          }
          for (int j = 0; j < this.columnContentWidth - substr.length(); j++) {
            sb.append(PADDING_CHAR);
          }
          indexes[i] = cells[i].length();
        } else {
          String substr = cells[i].substring(indexes[i],
            indexes[i] + this.columnContentWidth);
          indexes[i] += this.columnContentWidth;
          sb.append(substr);
        }
      }
      sb.append(COLUMN_CLOSER);
      getOut().write(sb.toString());
      getOut().write(LINE_SEPARATOR);
      getOut().flush();
    }
    printRowDelimiter(cells.length);
  }

  public void footer() throws IOException {
    if (isNoFormatting()) {
      // If no formatting, output a newline to delimit cell and the
      // result summary output at end of every command.
      getOut().write(LINE_SEPARATOR);
      getOut().flush();
    }
    // We're done. Clear flag.
    setNoFormatting(false);
  }
  
  private void printRowDelimiter(final int columnCount) throws IOException {
    for (int i = 0; i < columnCount; i++) {
      getOut().write(this.columnHorizLine);

    }
    getOut().write(COLUMN_HORIZ_LINE_CLOSER);
    getOut().write(LINE_SEPARATOR);
    getOut().flush();
  }
  
  private String calculateColumnHorizLine(final int width) {
    StringBuffer sb = new StringBuffer();
    sb.append("+");
    for (int i = 1; i < width; i++) {
      sb.append("-");
    }
    return sb.toString();
  }
  
  public boolean isNoFormatting() {
    return this.noFormatting;
  }

  public void setNoFormatting(boolean noFormatting) {
    this.noFormatting = noFormatting;
  }
}