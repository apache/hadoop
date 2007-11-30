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

import java.io.Writer;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.shell.algebra.Constants;

/**
 * This class represents a substitute command.
 */
public class SubstituteCommand extends BasicCommand {
  private String key;
  private String chainKey;
  private String operation;
  private String condition;

  public SubstituteCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(HBaseConfiguration conf) {
    VariableRef formula = new VariableRef(operation, condition);
    VariablesPool.put(key, chainKey, formula);
    return null;
  }

  public void setInput(String input) {
    this.operation = "table";
    this.condition = input;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setChainKey(String chainKey) {
    this.chainKey = chainKey;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public void resetVariableRelation(String r1, String r2) {
    setChainKey(r1);
    String tableName = VariablesPool.get(r1).get(null).getArgument();
    VariableRef formula = new VariableRef(Constants.JOIN_SECOND_RELATION,
        tableName);
    VariablesPool.put(r1, r2, formula);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SHELL;
  }
}
