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

import java.io.Writer;
import java.util.List;

import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Run hadoop filesystem commands.
 */
public class FsCommand extends BasicCommand {
  private List<String> query;

  public FsCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(@SuppressWarnings("unused")
  HBaseConfiguration conf) {
    // This commmand will write the
    FsShell shell = new FsShell();
    try {
      ToolRunner.run(shell, getQuery());
      shell.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public void setQuery(List<String> query) {
    this.query = query;
  }

  private String[] getQuery() {
    return query.toArray(new String[] {});
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SHELL;
  }
}
