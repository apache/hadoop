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

import java.util.HashMap;
import java.util.Map;

public class HelpContents {

  /**
   * add help contents 
   */
  public static Map<? extends String, ? extends String[]> Load() {
    Map<String, String[]> load = new HashMap<String, String[]>();

    load.put("SHOW", new String[] { "List all tables.", "SHOW TABLES;" });
    load.put("CLEAR", new String[] {"Clear the screen.", "CLEAR;"} );
    load.put("DESCRIBE", new String[] { "Describe a table's columnfamilies.",
        "DESCRIBE <table_name>;" });
    load.put("CREATE", new String[] {
        "Create a table",
        "CREATE <table_name>"
            + "\n\t  COLUMNFAMILIES('cf_name1'[, 'cf_name2', ...]);"
            + "\n    [LIMIT=versions_limit];" });
    load.put("DROP", new String[] {
        "Drop columnfamilie(s) from a table or drop table(s)",
        "DROP table_name1[, table_name2, ...] | cf_name1[, cf_name2, ...];" });
    load.put("INSERT", new String[] {
        "Insert row into table",
        "INSERT <table_name>" + "\n\t('column_name1'[, 'column_name2', ...])"
            + "\n\t    VALUES('entry1'[, 'entry2', ...])"
            + "\n    WHERE row='row_key';" });
    load.put("DELETE", new String[] {
        "Delete cell or row in table.",
        "DELETE <table_name>" + "\n\t    WHERE row='row_key;"
            + "\n    [AND column='column_name'];" });
    load.put("SELECT",
        new String[] {
            "Select values from a table",
            "SELECT <table_name>" + "\n\t    [WHERE row='row_key']"
                + "\n    [AND column='column_name'];"
                + "\n    [AND time='timestamp'];"
                + "\n    [LIMIT=versions_limit];" });
    load.put("EXIT", new String[] { "Exit shell", "EXIT;" });

    return load;
  }

}
