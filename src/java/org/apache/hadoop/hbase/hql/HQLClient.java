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

/**
 * HQL query language service client interfaces.
 */
public class HQLClient {
  static HBaseConfiguration conf;
  static TableFormatter tableFormatter = null;
  static Writer out = null;

  /**
   * Constructor
   *  
   * @param config HBaseConfiguration
   * @param ip IP Address
   * @param port port number
   * @param writer writer
   * @param formatter table formatter
   */
  public HQLClient(HBaseConfiguration config, String ip, int port, Writer writer,
      TableFormatter formatter) {
    conf = config;
    if (ip != null && port != -1)
      conf.set("hbase.master", ip + ":" + port);
    out = writer;
    tableFormatter = formatter;
  }

  /**
   * Executes query.
   * 
   * @param query
   * @return ReturnMsg object
   */
  public ReturnMsg executeQuery(String query) {
    HQLParser parser = new HQLParser(query, out, tableFormatter);
    ReturnMsg msg = null;

    try {
      Command cmd = parser.terminatedCommand();
      if (cmd != null) {
        msg = cmd.execute(conf);
      }
    } catch (ParseException pe) {
      msg = new ReturnMsg(Constants.ERROR_CODE,
          "Syntax error : Type 'help;' for usage.");
    } catch (TokenMgrError te) {
      msg = new ReturnMsg(Constants.ERROR_CODE,
          "Lexical error : Type 'help;' for usage.");
    }

    return msg;
  }
}
