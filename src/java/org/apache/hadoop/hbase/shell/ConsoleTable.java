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
  static {
    try {
      out = new PrintStream(System.out, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }
  
  public static void printHead(String name) {
    out.println("+------+----------------------+");
    out.print("| No.  | ");
    out.printf("%-20s", name);
    out.println(" |");
  }

  public static void printFoot() {
    out.println("+------+----------------------+");
    out.println();
  }

  public static void printTable(int count, String name) {
    out.println("+------+----------------------+");

    if (name.length() > 20) {
      int interval = 20;

      out.print("| ");
      out.printf("%-4s", count + 1);
      out.print(" | ");
      out.printf("%-20s", name.substring(0, interval));
      out.println(" |");

      for (int i = 0; i < name.length() / interval; i++) {
        out.print("| ");
        out.printf("%-4s", "");
        out.print(" | ");

        int end = ((interval * i) + interval + interval);
        if (end > name.length()) {
          out.printf("%-20s", name.substring(end - interval,
            name.length()));
        } else {
          out.printf("%-20s", name.substring(end - interval, end));
        }
        out.println(" |");
      }

    } else {
      out.print("| ");
      out.printf("%-4s", count + 1);
      out.print(" | ");
      out.printf("%-20s", name);
      out.println(" |");
    }
  }

  public static void selectHead() {
    out.println("+------+----------------------+" +
      "----------------------+----------------------+");
    out.print("| No.  | ");
    out.printf("%-20s", "Row");
    out.printf(" | ");
    out.printf("%-20s", "Column");
    out.printf(" | ");
    out.printf("%-20s", "Cell");
    out.println(" | ");
  }

  public static void printLine(int count, String key, String column,
      String cellData) {
    out.println("+------+----------------------+" +
      "----------------------+----------------------+");

    if (key.length() > 20 || column.length() > 20 || cellData.length() > 20) {
      int interval = 20;
      out.print("| ");
      out.printf("%-4s", count + 1);
      out.print(" | ");
      if (key.length() > 20)
        out.printf("%-20s", key.substring(0, interval));
      else
        out.printf("%-20s", key);
      out.print(" | ");
      if (column.length() > 20)
        out.printf("%-20s", column.substring(0, interval));
      else
        out.printf("%-20s", column);
      out.print(" | ");
      if (cellData.length() > 20)
        out.printf("%-20s", cellData.substring(0, interval));
      else
        out.printf("%-20s", cellData);
      out.println(" |");

      // out.println(getBiggerInt(new int[]{ 3, 1, 9}));
      int biggerStrLength = getBiggerInt(new int[] { key.length(),
        column.length(), cellData.length() });

      for (int i = 0; i < (biggerStrLength / interval); i++) {
        out.print("| ");
        out.printf("%-4s", "");
        out.print(" | ");

        int end = ((interval * i) + interval + interval);

        if (end > key.length()) {
          if (key.length() > interval && end - interval < key.length()) {
            out.printf("%-20s", key.substring(end - interval,
              key.length()));
          } else {
            out.printf("%-20s", "");
          }
        } else {
          out.printf("%-20s", key.substring(end - interval, end));
        }

        out.print(" | ");

        if (end > column.length()) {
          if (column.length() > interval && end - interval < column.length()) {
            out.printf("%-20s", column.substring(end - interval,
              column.length()));
          } else {
            out.printf("%-20s", "");
          }
        } else {
          out.printf("%-20s", column.substring(end - interval, end));
        }

        out.print(" | ");
        if (end > cellData.length()) {
          if (cellData.length() > interval &&
              end - interval < cellData.length()) {
            out.printf("%-20s",
              cellData.substring(end - interval, cellData.length()));
          } else {
            out.printf("%-20s", "");
          }
        } else {
          out.printf("%-20s", cellData.substring(end - interval, end));
        }
        out.println(" |");
      }

    } else {
      out.print("| ");
      out.printf("%-4s", count + 1);
      out.print(" | ");
      out.printf("%-20s", key);
      out.print(" | ");
      out.printf("%-20s", column);
      out.print(" | ");
      out.printf("%-20s", cellData);
      out.println(" |");
    }
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
    out.println("+------+----------------------+" +
      "----------------------+----------------------+");
    out.println();
  }
  
}
