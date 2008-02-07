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
package org.apache.hadoop.hbase.hql;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.VersionInfo;

public class HelpCommand extends BasicCommand {
  private String argument;
  private static final String[] HEADER = new String[] { "Command",
      "Description", "Example" };

  /** application name */
  public static final String APP_NAME = "HQL";

  /** help contents map */
  public final Map<String, String[]> help = new HashMap<String, String[]>();

  private final TableFormatter formatter;

  public HelpCommand(final Writer o, final TableFormatter f) {
    super(o);
    this.help.putAll(load());
    this.formatter = f;
  }

  public ReturnMsg execute(@SuppressWarnings("unused")
  HBaseConfiguration conf) {
    try {
      printHelp(this.argument);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void setArgument(String argument) {
    this.argument = argument;
  }

  /**
   * add help contents
   */
  private Map<? extends String, ? extends String[]> load() {
    Map<String, String[]> load = new HashMap<String, String[]>();
    load.put("SHOW", new String[] { "Show information about selected title",
        "SHOW TABLES [or substitution variable name];" });

    load.put("FS", new String[] {
        "Hadoop FsShell; entering a lone 'FS;' " + "will emit usage",
        "FS [-option] arguments..;" });

    load.put("JAR", new String[] { "Hadoop RunJar util",
        "JAR jarFile [mainClass] arguments...;" });
    load.put("CLEAR", new String[] { "Clear the screen", "CLEAR;" });

    load.put("DESCRIBE", new String[] { "Print table information",
        "[DESCRIBE|DESC] table_name;" });

    load
        .put(
            "CREATE",
            new String[] {
                "Create tables",
                "CREATE TABLE table_name (column_family_name [MAX_VERSIONS=n] "
                    + "[MAX_LENGTH=n] [COMPRESSION=NONE|RECORD|BLOCK] [IN_MEMORY] "
                    + "[BLOOMFILTER=NONE|BLOOMFILTER|COUNTING_BLOOMFILTER|RETOUCHED_BLOOMFILTER "
                    + "VECTOR_SIZE=n NUM_HASH=n], " + "...)" });
    load.put("DROP", new String[] { "Drop tables",
        "DROP TABLE table_name [, table_name] ...;" });

    load.put("INSERT", new String[] {
        "Insert values into table",
        "INSERT INTO table_name (column_name, ...) "
            + "VALUES ('value', ...) WHERE row='row_key'" +
            " [TIMESTAMP 'timestamp'];" });

    load.put("DELETE", new String[] {
        "Delete table data",
        "DELETE {column_name, [, column_name] ... | *} FROM table_name "
            + "WHERE row='row-key';" });

    load.put("SELECT", new String[] {
        "Select values from table",
        "SELECT {column_name, [, column_name] ... | expr[alias] | * } FROM table_name "
            + "[WHERE row='row_key' | STARTING FROM 'row-key' [UNTIL 'stop-key']] "
            + "[NUM_VERSIONS = version_count] " + "[TIMESTAMP 'timestamp'] "
            + "[LIMIT = row_count] " + "[INTO FILE 'file_name'];" });

    load.put("ALTER", new String[] {
        "Alter structure of table",
        "ALTER TABLE table_name ADD column_spec | "
            + "ADD (column_spec, column_spec, ...) | "
            + "CHANGE column_family column_spec | "
            + "DROP column_family_name | " + "CHANGE column_spec;" });

    load.put("TRUNCATE", new String[] {
         "Truncate table is used to clean all data from a table",
         "TRUNCATE TABLE table_name;" });
    
    load.put("EXIT", new String[] { "Exit shell", "EXIT;" });

    return load;
  }

  /**
   * Print out the program version.
   * 
   * @throws IOException
   */
  public void printVersion() throws IOException {
    println(APP_NAME + ", " + VersionInfo.getVersion() + " version.\n"
        + "Copyright (c) 2008 by udanax, "
        + "licensed to Apache Software Foundation.\n"
        + "Type 'help;' for usage.\n");
  }

  public void printHelp(final String cmd) throws IOException {
    if (cmd.equals("")) {
      println("Type 'help COMMAND;' to see command-specific usage.");
      printHelp(this.help);
    } else {
      if (this.help.containsKey(cmd.toUpperCase())) {
        final Map<String, String[]> m = new HashMap<String, String[]>();
        m.put(cmd.toUpperCase(), this.help.get(cmd.toUpperCase()));
        printHelp(m);
      } else {
        println("Unknown Command : Type 'help;' for usage.");
      }
    }
  }

  private void printHelp(final Map<String, String[]> m) throws IOException {
    this.formatter.header(HEADER);
    for (Map.Entry<String, String[]> e : m.entrySet()) {
      String[] value = e.getValue();
      if (value.length == 2) {
        this.formatter.row(new String[] { e.getKey().toUpperCase(), value[0],
            value[1] });
      } else {
        throw new IOException("Value has too many elements:" + value);
      }
    }
    this.formatter.footer();
  }

  public static void main(String[] args) throws UnsupportedEncodingException {
    HBaseConfiguration conf = new HBaseConfiguration();
    Writer out = new OutputStreamWriter(System.out, "UTF-8");
    TableFormatterFactory tff = new TableFormatterFactory(out, conf);
    HelpCommand cmd = new HelpCommand(out, tff.get());
    cmd.setArgument("");
    cmd.execute(conf);
    cmd.setArgument("select");
    cmd.execute(conf);
  }
}
