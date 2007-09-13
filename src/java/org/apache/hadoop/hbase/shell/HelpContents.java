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

    String columnName = "column_name: " 
      + "\n\t  column_family_name"
      + "\n\t| column_family_name:column_label_name";    
    String columnList = "{column_name, [, column_name] ... | *}";

    load.put("SHOW", new String[] {"List all available tables", "SHOW TABLES;"});

    load.put("FS", new String[] { "Hadoop FsShell operations.",
      "FS -copyFromLocal /home/user/backup.dat fs/user/backup;" });
    
    load.put("CLEAR", new String[] {"Clear the screen", "CLEAR;"} );
    
    load.put("DESCRIBE", new String[] { "Print information about tables",
    "[DESCRIBE|DESC] table_name;" });
    
    load.put("CREATE", new String[] {
        "Create tables",
        "CREATE TABLE table_name"
            + "\n\t(column_family_spec [, column_family_spec] ...);"
            + "\n\n"
      + "column_family_spec:"
      + "\n\tcolumn_family_name"
      + "\n\t[MAX_VERSIONS=n]"
      + "\n\t[MAX_LENGTH=n]"
      + "\n\t[COMPRESSION=NONE|RECORD|BLOCK]"
      + "\n\t[IN_MEMORY]"
      + "\n\t[BLOOMFILTER=NONE|BLOOM|COUNTING|RETOUCHED VECTOR_SIZE=n NUM_HASH=n]"
    });
    
    load.put("DROP", new String[] {
        "Drop tables",
        "DROP TABLE table_name [, table_name] ...;" });
    
    load.put("INSERT", new String[] {
        "Insert values into tables",
        "INSERT INTO table_name"
            + "\n\t(column_name, ...) VALUES ('value', ...)"
            + "\n\tWHERE row='row_key';"
            + "\n\n" + columnName            
    });
    
    load.put("DELETE", new String[] {
        "Delete a subset of the data in a table",
        "DELETE " + columnList 
            + "\n\tFROM table_name"
            + "\n\tWHERE row='row-key';" 
            + "\n\n"
            + columnName
    });
    
    load.put("SELECT",
        new String[] {
            "Select values from tables",
            "SELECT " + columnList + " FROM table_name" 
                + "\n\t[WHERE row='row_key' | STARTING FROM 'row-key']"
                + "\n\t[NUM_VERSIONS = version_count]"
                + "\n\t[TIMESTAMP 'timestamp']"
                + "\n\t[LIMIT = row_count]"
                + "\n\t[INTO FILE 'file_name'];"
    });
                
    load.put("ALTER",
        new String[] {
            "Alter the structure of a table",
            "ALTER TABLE table_name" 
                + "\n\t  ADD column_spec"
                + "\n\t| ADD (column_spec, column_spec, ...)"
                + "\n\t| DROP column_family_name"
                + "\n\t| CHANGE column_spec;" 
    });

    load.put("EXIT", new String[] { "Exit shell", "EXIT;" });

    return load;
  }

}
