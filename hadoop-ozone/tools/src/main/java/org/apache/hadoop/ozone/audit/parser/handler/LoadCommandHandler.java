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

import java.util.concurrent.Callable;

/**
 * Load command handler for ozone audit parser.
 */
@Command(name = "load",
    aliases = "l",
    description = "Load ozone audit log files",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class LoadCommandHandler implements Callable<Void> {

  @Parameters(arity = "1..1", description = "Audit Log file(s)")
  private String logs;

  @ParentCommand
  private AuditParser auditParser;

  public Void call() {
    if(DatabaseHelper.setup(auditParser.getDatabase(), logs)) {
      System.out.println(logs + " has been loaded successfully");
    } else {
      System.out.println("Failed to load " + logs);
    }
    return null;
  }
}
