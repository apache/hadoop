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
package org.apache.hadoop.hdfs.tools;

import java.util.LinkedList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * This class implements a "table listing" with column headers.
 */
@InterfaceAudience.Private
public class TableListing {
  public enum Justification {
    LEFT,
    RIGHT;
  }

  private static class Column {
    private final LinkedList<String> rows;
    private final Justification justification;
    private int maxLength;

    Column(String title, Justification justification) {
      this.rows = new LinkedList<String>();
      this.justification = justification;
      this.maxLength = 0;
      addRow(title);
    }

    private void addRow(String val) {
      if ((val.length() + 1) > maxLength) {
        maxLength = val.length() + 1;
      }
      rows.add(val);
    }

    String getRow(int i) {
      String raw = rows.get(i);
      int paddingLength = maxLength - raw.length();
      String padding = (paddingLength <= 0) ? "" :
        StringUtils.repeat(" ", paddingLength);
      if (justification == Justification.LEFT) {
        return raw + padding;
      } else {
        return padding + raw;
      }
    }
  }

  public static class Builder {
    private final LinkedList<Column> columns = new LinkedList<Column>();

    /**
     * Create a new Builder.
     */
    public Builder() {
    }

    /**
     * Add a new field to the Table under construction.
     *
     * @param title          Field title.
     * @param leftJustified  Whether or not the field is left justified.
     * @return               this.
     */
    public Builder addField(String title, Justification justification) {
      columns.add(new Column(title, justification));
      return this;
    }

    /**
     * Create a new TableListing.
     */
    public TableListing build() {
      return new TableListing(columns.toArray(new Column[0]));
    }
  }

  private final Column columns[];

  private int numRows;

  TableListing(Column columns[]) {
    this.columns = columns;
    this.numRows = 0;
  }

  /**
   * Add a new row.
   *
   * @param row    The row of objects to add-- one per column.
   */
  public void addRow(String row[]) {
    if (row.length != columns.length) {
      throw new RuntimeException("trying to add a row with " + row.length +
            " columns, but we have " + columns.length + " columns.");
    }
    for (int i = 0; i < columns.length; i++) {
      columns[i].addRow(row[i]);
    }
    numRows++;
  }

  /**
   * Convert the table to a string.
   */
  public String build() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < numRows + 1; i++) {
      String prefix = "";
      for (int j = 0; j < columns.length; j++) {
        builder.append(prefix);
        prefix = " ";
        builder.append(columns[j].getRow(i));
      }
      builder.append("\n");
    }
    return builder.toString();
  }
}
