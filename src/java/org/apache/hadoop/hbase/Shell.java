/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import jline.ConsoleReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.shell.Command;
import org.apache.hadoop.hbase.shell.HelpManager;
import org.apache.hadoop.hbase.shell.ReturnMsg;
import org.apache.hadoop.hbase.shell.generated.ParseException;
import org.apache.hadoop.hbase.shell.generated.Parser;
import org.apache.hadoop.hbase.shell.generated.TokenMgrError;

/**
 * An hbase shell.
 * 
 * @see <a href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseShell">HBaseShell</a>
 */
public class Shell {
  /** audible keyboard bells */
  public static final boolean DEFAULT_BELL_ENABLED = true;

  /** Main method */
  public static void main(String args[]) throws IOException {
    Configuration conf = new HBaseConfiguration();
    HClient client = new HClient(conf);
    ConsoleReader reader = new ConsoleReader();
    reader.setBellEnabled(conf.getBoolean("hbaseshell.jline.bell.enabled",
        DEFAULT_BELL_ENABLED));
    HelpManager help = new HelpManager();
    help.printVersion();
    StringBuilder queryStr = new StringBuilder();
    String extendedLine;
    while ((extendedLine = reader.readLine(getPrompt(queryStr))) != null) {
      if (isEndOfCommand(extendedLine)) {
        queryStr.append(" " + extendedLine);
        long start = System.currentTimeMillis();
        Parser parser = new Parser(queryStr.toString());
        ReturnMsg rs = null;
        try {
          Command cmd = parser.terminatedCommand();
          if (cmd != null) {
            rs = cmd.execute(client);
          }
        } catch (ParseException pe) {
          String[] msg = pe.getMessage().split("[\n]");
          System.out.println("Syntax error : Type 'help' for usage: " + msg[0]);
        } catch (TokenMgrError te) {
          System.out.println("Lexical error : Type 'help' for usage.");
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

  /** Return the boolean value indicating whether end of command or not */
  static boolean isEndOfCommand(String line) {
    return (line.lastIndexOf(';') > -1) ? true : false;
  }

  /** Return the string of prompt start string */
  private static String getPrompt(final StringBuilder queryStr) {
    return (queryStr.toString().equals("")) ? "HBase > " : "    --> ";
  }

  /** return a string of code execution time. */
  public static String executeTime(boolean watch, long start, long end) {
    return (watch) ? "(" + String.format("%.2f", (end - start) * 0.001) + " sec)" : "";
  }
}
