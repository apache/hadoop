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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import jline.ConsoleReader;

import org.apache.hadoop.hbase.hql.Constants;
import org.apache.hadoop.hbase.hql.HQLClient;
import org.apache.hadoop.hbase.hql.HQLSecurityManager;
import org.apache.hadoop.hbase.hql.HelpCommand;
import org.apache.hadoop.hbase.hql.ReturnMsg;
import org.apache.hadoop.hbase.hql.TableFormatter;
import org.apache.hadoop.hbase.hql.TableFormatterFactory;
import org.apache.hadoop.hbase.hql.formatter.HtmlTableFormatter;

/**
 * An hbase shell.
 * 
 * @see <a
 *      href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseShell">HbaseShell</a>
 */
public class Shell {
  /** audible keyboard bells */
  public static final boolean DEFAULT_BELL_ENABLED = true;
  public static String IP = null;
  public static int PORT = -1;
  public static String HTML_OPTION = null;

  /** Return the boolean value indicating whether end of command or not */
  static boolean isEndOfCommand(String line) {
    return (line.lastIndexOf(';') > -1) ? true : false;
  }

  /** Return the string of prompt start string */
  private static String getPrompt(final StringBuilder queryStr) {
    return (queryStr.toString().equals("")) ? "hql > " : "  --> ";
  }

  /**
   * @param watch true if execution time should be computed and returned
   * @param start start of time interval
   * @param end end of time interval
   * @return a string of code execution time.
   */
  public static String executeTime(boolean watch, long start, long end) {
    return watch ? " ("
        + String.format("%.2f", Double.valueOf((end - start) * 0.001)) + " sec)"
        : "";
  }

  /**
   * Main method
   * 
   * @param args not used
   * @throws IOException
   */
  public static void main(String args[]) throws IOException {
    argumentParsing(args);
    if (args.length != 0) {
      if (args[0].equals("--help") || args[0].equals("-h")) {
        System.out
            .println("Usage: ./bin/hbase shell [--master:master_address:port] [--html]\n");
        System.exit(1);
      }
    }

    HBaseConfiguration conf = new HBaseConfiguration();
    ConsoleReader reader = new ConsoleReader();
    System.setSecurityManager(new HQLSecurityManager());
    reader.setBellEnabled(conf.getBoolean("hbaseshell.jline.bell.enabled",
        DEFAULT_BELL_ENABLED));
    Writer out = new OutputStreamWriter(System.out, "UTF-8");
    TableFormatter tableFormatter = new TableFormatterFactory(out, conf).get();

    if (HTML_OPTION != null) {
      tableFormatter = new HtmlTableFormatter(out);
    }

    HelpCommand help = new HelpCommand(out, tableFormatter);
    if (args.length == 0 || !args[0].equals(String.valueOf(Constants.FLAG_RELAUNCH))) {
      help.printVersion();
    }

    StringBuilder queryStr = new StringBuilder();
    String extendedLine;
    HQLClient hql = new HQLClient(conf, IP, PORT, out, tableFormatter);

    while ((extendedLine = reader.readLine(getPrompt(queryStr))) != null) {
      if (isEndOfCommand(extendedLine)) {
        queryStr.append(" " + extendedLine);
        long start = System.currentTimeMillis();

        ReturnMsg rs = hql.executeQuery(queryStr.toString());

        long end = System.currentTimeMillis();
        if (rs != null) {
          if (rs != null && rs.getType() > Constants.ERROR_CODE)
            System.out.println(rs.getMsg() +
              executeTime((rs.getType() == 1), start, end));
          else if (rs.getType() == Constants.ERROR_CODE)           
            System.out.println(rs.getMsg());
        }
        queryStr = new StringBuilder();
      } else {
        queryStr.append(" " + extendedLine);
      }
    }
    System.out.println();
  }

  private static void argumentParsing(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].toLowerCase().startsWith("--master:")) {
        String[] address = args[i].substring(9, args[i].length()).split(":");
        IP = address[0];
        PORT = Integer.valueOf(address[1]);
      } else if (args[i].toLowerCase().startsWith("--html")) {
        HTML_OPTION = args[i];
      }
    }
  }
}
