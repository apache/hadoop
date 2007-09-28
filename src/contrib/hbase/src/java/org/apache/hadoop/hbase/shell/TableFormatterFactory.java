/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.shell;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

/**
 * Table formatter.
 * TODO: Make a factory that chooses the formatter to use based off
 * configuration.  Read a property from hbase-site or from System properties.
 * For now, default is the internal AsciiTableFormatter.
 * TODO: Mysql has --skip-column-names and --silent which inserts a tab as
 * separator.  Also has --html and --xml.
 */
public class TableFormatterFactory {
  private static final TableFormatterFactory factory =
    new TableFormatterFactory();
  private final TableFormatter formatter;
  
  private TableFormatterFactory() {
    this.formatter = new AsciiTableFormatter();
  }
  
  /**
   * @return Configured table formatter.
   */
  public static TableFormatter get() {
    return factory.formatter;
  }
  
  /*
   * Formmatter that outputs data in UTF-8 inside an ASCII table on STDOUT.
   * If only a single cell result, then no formatting is done.  Presumption is
   * that client manages serial access outputting tables.
   */
  private class AsciiTableFormatter implements TableFormatter {
    private PrintStream out;
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
    
    /*
     * Constructor. 
     */
    protected AsciiTableFormatter() {
      try {
        this.out = new PrintStream(System.out, true, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed setting output to UTF-8", e);
      }
    }
    
    /**
     * @param titles List of titles.  Pass null if no formatting (i.e.
     * no header, no footer, etc.
     */
    public void header(String[] titles) {
      if (titles == null) {
        // print nothing.
        this.noFormatting = true;
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

    public void row(String [] cells) {
      if (this.noFormatting) {
        this.out.print(cells[0]);
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
        this.out.println(sb.toString());
      }
      printRowDelimiter(cells.length);
    }

    public void footer() {
      if (this.noFormatting) {
        // If no formatting, output a newline to delimit cell and the
        // result summary output at end of every command.
        this.out.println();
      }
      // We're done. Clear flag.
      this.noFormatting = false;
    }
    
    private void printRowDelimiter(final int columnCount) {
      for (int i = 0; i < columnCount; i++) {
        this.out.print(this.columnHorizLine);
      }
      this.out.println(COLUMN_HORIZ_LINE_CLOSER);
    }
    
    private String calculateColumnHorizLine(final int width) {
      StringBuffer sb = new StringBuffer();
      sb.append("+");
      for (int i = 1; i < width; i++) {
        sb.append("-");
      }
      return sb.toString();
    }
  }
}