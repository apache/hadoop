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
 * Manufactures console table, but stupid.
 */
public class ConsoleTable {
  
  private static PrintStream out;
  private static final String sBar = "+------+----------------------+";
  private static final String lBar = "----------------------+----------------------+";
  
  static {
    try {
      out = new PrintStream(System.out, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }
  
  public static void printHead(String name) {
    out.println(sBar);
    out.print("| No.  | ");
    printCell(name, " |", true);
  }

  public static void printFoot() {
    out.println(sBar);
    out.println();
  }

  public static void printTable(int count, String name) {
    out.println(sBar);
    if (name.length() > 20) {
      int interval = 20;
      out.print("| ");
      out.printf("%-4s", Integer.valueOf(count + 1));
      out.print(" | ");
      printCell(name.substring(0, interval), " |", true);
      for (int i = 0; i < name.length() / interval; i++) {
        out.print("| ");
        out.printf("%-4s", "");
        out.print(" | ");

        int end = ((interval * i) + interval + interval);
        if (end > name.length()) {
          printCell(name.substring(end - interval, name.length()), " |", true);
        } else {
          printCell(name.substring(end - interval, end), " |", true);
        }
      }
    } else {
      out.print("| ");
      out.printf("%-4s", Integer.valueOf(count + 1));
      out.print(" | ");
      printCell(name, " |", true);
    }
  }

  public static void selectHead() {
    out.println(sBar + lBar);
    out.print("| No.  | ");
    printCell("Row", " | ", false);
    printCell("Column", " | ", false);
    printCell("Cell", " | ", true);
  }

  public static void printLine(int count, String key, String column,
      String cellData) {
    out.println(sBar + lBar);
    if (key.length() > 20 || column.length() > 20 || cellData.length() > 20) {
      int interval = 20;
      out.print("| ");
      out.printf("%-4s", Integer.valueOf(count + 1));
      out.print(" | ");

      printLongCell(key, interval);
      printLongCell(column, interval);
      printLongCell(cellData, interval);

      int biggerStrLength = getBiggerInt(new int[] { key.length(),
          column.length(), cellData.length() });

      for (int i = 0; i < (biggerStrLength / interval); i++) {
        out.print("| ");
        out.printf("%-4s", "");
        out.print(" | ");

        int end = ((interval * i) + interval + interval);

        printLongCellData(key, end, interval, false);
        printLongCellData(column, end, interval, false);
        printLongCellData(cellData, end, interval, false);
      }
    } else {
      out.print("| ");
      out.printf("%-4s", Integer.valueOf(count + 1));
      out.print(" | ");
      printCell(key, " | ", false);
      printCell(column, " | ", false);
      printCell(cellData, " |", true);
    }
  }

  private static void printLongCellData(String key, int end, int interval,
      boolean newLine) {
    if (end > key.length()) {
      if (key.length() > interval && end - interval < key.length()) {
        out.printf("%-20s", key.substring(end - interval, key.length()));
      } else {
        out.printf("%-20s", "");
      }
    } else {
      out.printf("%-20s", key.substring(end - interval, end));
    }
    out.print(" | ");
    if (newLine)
      out.println();
  }

  private static void printLongCell(String iKey, int interval) {
    if (iKey.length() > 20)
      printCell(iKey.substring(0, interval), " | ", true);
    else
      printCell(iKey, " | ", true);
  }

  private static void printCell(String data, String end, boolean newLine) {
    out.printf("%-20s", data);
    out.printf(end);
    if (newLine)
      out.println();
  }

  public static int getBiggerInt(int[] integers) {
    int result = -1;
    for (int i = 0; i < integers.length; i++) {
      if (integers[i] > result) {
        result = integers[i];
      }
    }
    return result;
  }

  public static void selectFoot() {
    out.println(sBar + lBar);
    out.println();
  }
}