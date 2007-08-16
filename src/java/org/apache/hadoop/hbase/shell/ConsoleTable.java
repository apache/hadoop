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

/**
 * Manufactures console table, but stupid.
 */
public class ConsoleTable {
  
  public static void printHead(String name) {
    System.out.println("+------+----------------------+");
    System.out.print("| No.  | ");
    System.out.printf("%-20s", name);
    System.out.println(" |");
  }

  public static void printFoot() {
    System.out.println("+------+----------------------+");
    System.out.println();
  }

  public static void printTable(int count, String name) {
    System.out.println("+------+----------------------+");

    if (name.length() > 20) {
      int interval = 20;

      System.out.print("| ");
      System.out.printf("%-4s", count + 1);
      System.out.print(" | ");
      System.out.printf("%-20s", name.substring(0, interval));
      System.out.println(" |");

      for (int i = 0; i < name.length() / interval; i++) {
        System.out.print("| ");
        System.out.printf("%-4s", "");
        System.out.print(" | ");

        int end = ((interval * i) + interval + interval);
        if (end > name.length()) {
          System.out.printf("%-20s", name.substring(end - interval,
            name.length()));
        } else {
          System.out.printf("%-20s", name.substring(end - interval, end));
        }
        System.out.println(" |");
      }

    } else {
      System.out.print("| ");
      System.out.printf("%-4s", count + 1);
      System.out.print(" | ");
      System.out.printf("%-20s", name);
      System.out.println(" |");
    }
  }

  public static void selectHead() {
    System.out.println("+------+----------------------+" +
      "----------------------+----------------------+");
    System.out.print("| No.  | ");
    System.out.printf("%-20s", "Row");
    System.out.printf(" | ");
    System.out.printf("%-20s", "Column");
    System.out.printf(" | ");
    System.out.printf("%-20s", "Cell");
    System.out.println(" | ");
  }

  public static void printLine(int count, String key, String column,
      String cellData) {
    System.out.println("+------+----------------------+" +
      "----------------------+----------------------+");

    if (key.length() > 20 || column.length() > 20 || cellData.length() > 20) {
      int interval = 20;
      System.out.print("| ");
      System.out.printf("%-4s", count + 1);
      System.out.print(" | ");
      if (key.length() > 20)
        System.out.printf("%-20s", key.substring(0, interval));
      else
        System.out.printf("%-20s", key);
      System.out.print(" | ");
      if (column.length() > 20)
        System.out.printf("%-20s", column.substring(0, interval));
      else
        System.out.printf("%-20s", column);
      System.out.print(" | ");
      if (cellData.length() > 20)
        System.out.printf("%-20s", cellData.substring(0, interval));
      else
        System.out.printf("%-20s", cellData);
      System.out.println(" |");

      // System.out.println(getBiggerInt(new int[]{ 3, 1, 9}));
      int biggerStrLength = getBiggerInt(new int[] { key.length(),
        column.length(), cellData.length() });

      for (int i = 0; i < (biggerStrLength / interval); i++) {
        System.out.print("| ");
        System.out.printf("%-4s", "");
        System.out.print(" | ");

        int end = ((interval * i) + interval + interval);

        if (end > key.length()) {
          if (key.length() > interval && end - interval < key.length()) {
            System.out.printf("%-20s", key.substring(end - interval,
              key.length()));
          } else {
            System.out.printf("%-20s", "");
          }
        } else {
          System.out.printf("%-20s", key.substring(end - interval, end));
        }

        System.out.print(" | ");

        if (end > column.length()) {
          if (column.length() > interval && end - interval < column.length()) {
            System.out.printf("%-20s", column.substring(end - interval,
              column.length()));
          } else {
            System.out.printf("%-20s", "");
          }
        } else {
          System.out.printf("%-20s", column.substring(end - interval, end));
        }

        System.out.print(" | ");
        if (end > cellData.length()) {
          if (cellData.length() > interval &&
              end - interval < cellData.length()) {
            System.out.printf("%-20s",
              cellData.substring(end - interval, cellData.length()));
          } else {
            System.out.printf("%-20s", "");
          }
        } else {
          System.out.printf("%-20s", cellData.substring(end - interval, end));
        }
        System.out.println(" |");
      }

    } else {
      System.out.print("| ");
      System.out.printf("%-4s", count + 1);
      System.out.print(" | ");
      System.out.printf("%-20s", key);
      System.out.print(" | ");
      System.out.printf("%-20s", column);
      System.out.print(" | ");
      System.out.printf("%-20s", cellData);
      System.out.println(" |");
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
    System.out.println("+------+----------------------+" +
      "----------------------+----------------------+");
    System.out.println();
  }
  
}
