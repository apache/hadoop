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

import org.apache.hadoop.hbase.shell.Command;
import org.apache.hadoop.hbase.shell.HelpCommand;
import org.apache.hadoop.hbase.shell.ReturnMsg;
import org.apache.hadoop.hbase.shell.ShellSecurityManager;
import org.apache.hadoop.hbase.shell.TableFormatter;
import org.apache.hadoop.hbase.shell.TableFormatterFactory;
import org.apache.hadoop.hbase.shell.formatter.HtmlTableFormatter;
import org.apache.hadoop.hbase.shell.generated.ParseException;
import org.apache.hadoop.hbase.shell.generated.Parser;
import org.apache.hadoop.hbase.shell.generated.TokenMgrError;

/**
 * An hbase shell.
 * 
 * @see <a
 *      href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseShell">HbaseShell</a>
 */
public class Shell {
  /** audible keyboard bells */
  public static final boolean DEFAULT_BELL_ENABLED = true;
  public static String MASTER_ADDRESS = null;
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
    HBaseConfiguration conf = new HBaseConfiguration();
    ConsoleReader reader = new ConsoleReader();
    System.setSecurityManager(new ShellSecurityManager());
    reader.setBellEnabled(conf.getBoolean("hbaseshell.jline.bell.enabled",
        DEFAULT_BELL_ENABLED));
    Writer out = new OutputStreamWriter(System.out, "UTF-8");
    TableFormatter tableFormater = new TableFormatterFactory(out, conf).get();
    if (MASTER_ADDRESS != null) {
      conf.set("hbase.master", MASTER_ADDRESS.substring(9, MASTER_ADDRESS.length()));
    }
    if (HTML_OPTION != null) {
      tableFormater = new HtmlTableFormatter(out);
      System.out.println("--html");
    }

    HelpCommand help = new HelpCommand(out, tableFormater);
    if (args.length == 0 || !args[0].equals("7")) {
      help.printVersion();
    }
    StringBuilder queryStr = new StringBuilder();
    String extendedLine;
    while ((extendedLine = reader.readLine(getPrompt(queryStr))) != null) {
      if (isEndOfCommand(extendedLine)) {
        queryStr.append(" " + extendedLine);
        long start = System.currentTimeMillis();
        Parser parser = new Parser(queryStr.toString(), out, tableFormater);
        ReturnMsg rs = null;
        try {
          Command cmd = parser.terminatedCommand();
          if (cmd != null) {
            rs = cmd.execute(conf);
          }
        } catch (ParseException pe) {
          String[] msg = pe.getMessage().split("[\n]");
          System.out.println("Syntax error : Type 'help;' for usage.\nMessage : "
              + msg[0]);
        } catch (TokenMgrError te) {
          String[] msg = te.getMessage().split("[\n]");
          System.out.println("Lexical error : Type 'help;' for usage.\nMessage : "
              + msg[0]);
        }

        long end = System.currentTimeMillis();
        if (rs != null && rs.getType() > -1)
          System.out.println(rs.getMsg()
              + executeTime((rs.getType() == 1), start, end));
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
        MASTER_ADDRESS = args[i];
      } else if (args[i].toLowerCase().startsWith("--html")) {
        HTML_OPTION = args[i];
      }
    }
  }
}
