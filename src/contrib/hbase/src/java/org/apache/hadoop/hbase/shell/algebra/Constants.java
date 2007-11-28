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
package org.apache.hadoop.hbase.shell.algebra;

/**
 * List of access control algebraic operations constants.
 */
public class Constants {
  public static final String OUTPUT_TABLE_EXIST = "job.config.output.table";
  public static final String CONFIG_INPUT = "input";
  public static final String CONFIG_OUTPUT = "output";
  public static final String EXPRESSION_FILTER_LIST = "expression.filter.list";
  
  public static final String RELATIONAL_PROJECTION = "projection";
  public static final String RELATIONAL_SELECTION = "selection";
  public static final String RELATIONAL_GROUP = "group";
  public static final String RELATIONAL_JOIN = "join";
  public static final String JOIN_SECOND_RELATION = "secondR";
}
