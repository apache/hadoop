/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.audit.parser.handler;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.audit.parser.AuditParser;
import org.apache.hadoop.ozone.audit.parser.common.DatabaseHelper;
import picocli.CommandLine.*;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Custom query command handler for ozone audit parser.
 * The query must be enclosed within double quotes.
 */
@Command(name = "query",
    aliases = "q",
    description = "Execute custom query",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class QueryCommandHandler implements Callable<Void> {

  @Parameters(arity = "1..1", description = "Custom query enclosed within " +
      "double quotes.")
  private String query;

  @ParentCommand
  private AuditParser auditParser;

  public Void call() {
    try {
      System.out.println(
          DatabaseHelper.executeCustomQuery(auditParser.getDatabase(), query)
      );
    } catch (SQLException ex) {
      System.err.println(ex.getMessage());
    }
    return null;
  }
}
