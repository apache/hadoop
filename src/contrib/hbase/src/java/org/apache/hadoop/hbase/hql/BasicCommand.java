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
import java.io.Writer;

/**
 * Takes the lowest-common-denominator {@link Writer} doing its own printlns,
 * etc.
 * 
 * @see <a
 *      href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseShell">HBaseShell</a>
 */
public abstract class BasicCommand implements Command, CommandFactory {
  private final Writer out;
  public final String LINE_SEPARATOR = System.getProperty("line.separator");
  public final String TABLE_NOT_FOUND = " is non-existant table.";

  // Shutdown constructor.
  @SuppressWarnings("unused")
  private BasicCommand() {
    this(null);
  }

  /**
   * Constructor
   * 
   * @param o A Writer.
   */
  public BasicCommand(final Writer o) {
    this.out = o;
  }

  public BasicCommand getBasicCommand() {
    return this;
  }

  /** basic commands are their own factories. */
  public Command getCommand() {
    return this;
  }

  protected String extractErrMsg(String msg) {
    int index = msg.indexOf(":");
    int eofIndex = msg.indexOf("\n");
    return msg.substring(index + 1, eofIndex);
  }

  protected String extractErrMsg(Exception e) {
    return extractErrMsg(e.getMessage());
  }

  /**
   * Appends, if it does not exist, a delimiter (colon) at the end of the column
   * name.
   */
  protected String appendDelimiter(String column) {
    return (!column.endsWith(FAMILY_INDICATOR) && column
        .indexOf(FAMILY_INDICATOR) == -1) ? column + FAMILY_INDICATOR : column;
  }

  /**
   * @return Writer to use outputting.
   */
  public Writer getOut() {
    return this.out;
  }

  public void print(final String msg) throws IOException {
    this.out.write(msg);
  }

  public void println(final String msg) throws IOException {
    print(msg);
    print(LINE_SEPARATOR);
    this.out.flush();
  }

  public CommandType getCommandType() {
    return CommandType.SELECT;
  }
}
