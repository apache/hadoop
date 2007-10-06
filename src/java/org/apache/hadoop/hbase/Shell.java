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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.shell.Command;
import org.apache.hadoop.hbase.shell.HelpCommand;
import org.apache.hadoop.hbase.shell.ReturnMsg;
import org.apache.hadoop.hbase.shell.TableFormatterFactory;
import org.apache.hadoop.hbase.shell.generated.ParseException;
import org.apache.hadoop.hbase.shell.generated.Parser;
import org.apache.hadoop.hbase.shell.generated.TokenMgrError;

/**
 * An hbase shell.
 * 
 * @see <a href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseShell">HbaseShell</a>
 */
public class Shell {
  /** audible keyboard bells */
  public static final boolean DEFAULT_BELL_ENABLED = true;
  

  /** Return the boolean value indicating whether end of command or not */
  static boolean isEndOfCommand(String line) {
    return (line.lastIndexOf(';') > -1) ? true : false;
  }

  /** Return the string of prompt start string */
  private static String getPrompt(final StringBuilder queryStr) {
    return (queryStr.toString().equals("")) ? "Hbase> " : "   --> ";
  }

  /**
   * @param watch true if execution time should be computed and returned
   * @param start start of time interval
   * @param end end of time interval
   * @return a string of code execution time. */
  public static String executeTime(boolean watch, long start, long end) {
    return watch?
      " (" + String.format("%.2f", Double.valueOf((end - start) * 0.001)) +
        " sec)":
      "";
  }

 
  /**
   * Main method
   * @param args not used
   * @throws IOException
   */
  public static void main(@SuppressWarnings("unused") String args[])
  throws IOException {
    Configuration conf = new HBaseConfiguration();
    ConsoleReader reader = new ConsoleReader();
    reader.setBellEnabled(conf.getBoolean("hbaseshell.jline.bell.enabled",
      DEFAULT_BELL_ENABLED));
    Writer out = new OutputStreamWriter(System.out, "UTF-8");
    TableFormatterFactory tff = new TableFormatterFactory(out, conf);
    HelpCommand help = new HelpCommand(out, tff.get());
    help.printVersion();
    StringBuilder queryStr = new StringBuilder();
    String extendedLine;
    while ((extendedLine = reader.readLine(getPrompt(queryStr))) != null) {
      if (isEndOfCommand(extendedLine)) {
        queryStr.append(" " + extendedLine);
        long start = System.currentTimeMillis();
        Parser parser = new Parser(queryStr.toString(), out, tff.get());
        ReturnMsg rs = null;
        try {
          Command cmd = parser.terminatedCommand();
          if (cmd != null) {
            rs = cmd.execute(conf);
          }
        } catch (ParseException pe) {
          String[] msg = pe.getMessage().split("[\n]");
          System.out.println("Syntax error : Type 'help;' for usage.\nMessage : " + msg[0]);
        } catch (TokenMgrError te) {
          String[] msg = te.getMessage().split("[\n]");
          System.out.println("Lexical error : Type 'help;' for usage.\nMessage : " + msg[0]);
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
}
