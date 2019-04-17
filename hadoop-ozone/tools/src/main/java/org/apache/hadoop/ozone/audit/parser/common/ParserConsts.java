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
package org.apache.hadoop.ozone.audit.parser.common;

/**
 * Constants used for ozone audit parser.
 */
public final class ParserConsts {

  private ParserConsts() {
    //Never constructed
  }

  public static final String DRIVER = "org.sqlite.JDBC";
  public static final String CONNECTION_PREFIX = "jdbc:sqlite:";
  public static final String DATE_REGEX = "^\\d{4}-\\d{2}-\\d{2}.*$";
  public static final String PROPS_FILE = "commands.properties";
  public static final String INSERT_AUDITS = "insertAuditEntry";
  public static final String CREATE_AUDIT_TABLE = "createAuditTable";

}
