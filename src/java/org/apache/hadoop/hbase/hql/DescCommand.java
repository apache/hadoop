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

import java.io.IOException;
import java.io.Writer;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.Text;

/**
 * Prints information about tables.
 */
public class DescCommand extends BasicCommand {
  private static final String[] HEADER = new String[] { "Column Family Descriptor" };
  private Text tableName;
  private final TableFormatter formatter;

  // Not instantiable
  @SuppressWarnings("unused")
  private DescCommand() {
    this(null, null);
  }

  public DescCommand(final Writer o, final TableFormatter f) {
    super(o);
    this.formatter = f;
  }

  public ReturnMsg execute(final HBaseConfiguration conf) {
    if (tableName == null)
      return new ReturnMsg(0, "Syntax error : Please check 'Describe' syntax.");
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      if (!admin.tableExists(tableName)) {
        return new ReturnMsg(0, "Table not found.");
      }
      HTableDescriptor[] tables = admin.listTables();
      HColumnDescriptor[] columns = null;
      for (int i = 0; i < tables.length; i++) {
        if (tables[i].getName().equals(tableName)) {
          columns = tables[i].getFamilies().values().toArray(
              new HColumnDescriptor[] {});
          break;
        }
      }
      formatter.header(HEADER);
      // Do a toString on the HColumnDescriptors
      String[] columnStrs = new String[columns.length];
      for (int i = 0; i < columns.length; i++) {
        String tmp = columns[i].toString();
        // Strip the curly-brackets if present.
        if (tmp.length() > 2 && tmp.startsWith("{") && tmp.endsWith("}")) {
          tmp = tmp.substring(1, tmp.length() - 1);
        }
        columnStrs[i] = tmp;
        formatter.row(new String[] { columnStrs[i] });
      }
      formatter.footer();
      return new ReturnMsg(1, columns.length + " columnfamily(s) in set.");
    } catch (IOException e) {
      return new ReturnMsg(0, "error msg : " + e.toString());
    }
  }

  public void setArgument(String table) {
    this.tableName = new Text(table);
  }
}
