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
package org.apache.hadoop.ozone.audit.parser;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.ozone.audit.parser.handler.LoadCommandHandler;
import org.apache.hadoop.ozone.audit.parser.handler.QueryCommandHandler;
import org.apache.hadoop.ozone.audit.parser.handler.TemplateCommandHandler;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;

/**
 * Ozone audit parser tool.
 */
@Command(name = "ozone auditparser",
    description = "Shell parser for Ozone Audit Logs",
    subcommands = {
        LoadCommandHandler.class,
        TemplateCommandHandler.class,
        QueryCommandHandler.class
    },
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class AuditParser extends GenericCli {
  /*
  <.db file path> load <file>
  <.db file path> template <template name>
  <.db file path> query <custom sql>
   */
  @Parameters(arity = "1..1", description = "Existing or new .db file")
  private String database;

  public static void main(String[] argv) throws Exception {
    new AuditParser().run(argv);
  }

  public String getDatabase(){
    return database;
  }
}
