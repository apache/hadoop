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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.hql.generated.HQLParser;
import org.apache.hadoop.hbase.hql.generated.ParseException;
import org.apache.hadoop.hbase.hql.generated.TokenMgrError;

public class HQLClient {
  public static String MASTER_ADDRESS = null;
  static HBaseConfiguration conf;
  static TableFormatter tableFormater;
  static Writer out;

  public HQLClient(HBaseConfiguration config, String master, Writer output,
      TableFormatter formatter) {
    conf = config;
    out = output;
    tableFormater = formatter;
    MASTER_ADDRESS = master;
  }

  public ReturnMsg executeQuery(String query) {
    HQLParser parser = new HQLParser(query, out, tableFormater);
    ReturnMsg rs = null;

    try {
      Command cmd = parser.terminatedCommand();
      if (cmd != null) {
        rs = cmd.execute(conf);
      }
    } catch (ParseException pe) {
      String[] msg = pe.getMessage().split("[\n]");
      rs = new ReturnMsg(-9, "Syntax error : Type 'help;' for usage.\nMessage : " + msg[0]);
    } catch (TokenMgrError te) {
      String[] msg = te.getMessage().split("[\n]");
      rs = new ReturnMsg(-9, "Lexical error : Type 'help;' for usage.\nMessage : " + msg[0]);
    }

    return rs;
  }
}