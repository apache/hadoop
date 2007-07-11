/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HClient;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;

public class SelectCommand extends BasicCommand {
  String table;

  int limit;

  Map<String, List<String>> condition;

  public ReturnMsg execute(HClient client) {
    if (this.condition != null && this.condition.containsKey("error"))
      return new ReturnMsg(0, "Syntax error : Please check 'Select' syntax.");

    try {
      client.openTable(new Text(this.table));

      switch (getCondition()) {
      case 0:

        HTableDescriptor[] tables = client.listTables();
        Text[] columns = null;

        if (this.table.equals(HConstants.ROOT_TABLE_NAME.toString())
            || this.table.equals(HConstants.META_TABLE_NAME.toString())) {
          columns = HConstants.COLUMN_FAMILY_ARRAY;
        } else {
          for (int i = 0; i < tables.length; i++) {
            if (tables[i].getName().toString().equals(this.table)) {
              columns = tables[i].families().keySet().toArray(new Text[] {});
            }
          }
        }

        HScannerInterface scan = client.obtainScanner(columns, new Text(""));
        HStoreKey key = new HStoreKey();
        TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();

        ConsoleTable.selectHead();
        int count = 0;
        while (scan.next(key, results)) {
          Text rowKey = key.getRow();

          for (Text columnKey : results.keySet()) {
            byte[] value = results.get(columnKey);
            String cellData = new String(value);

            if (columnKey.equals(HConstants.COL_REGIONINFO)) {
              DataInputBuffer inbuf = new DataInputBuffer();
              HRegionInfo info = new HRegionInfo();
              inbuf.reset(value, value.length);
              info.readFields(inbuf);

              cellData = "ID : " + String.valueOf(info.getRegionId());
            }
            ConsoleTable.printLine(count, rowKey.toString(), columnKey.toString(),
                cellData);
            count++;
          }
          results = new TreeMap<Text, byte[]>();
        }
        ConsoleTable.selectFoot();
        scan.close();

        break;

      case 1:

        count = 0;
        ConsoleTable.selectHead();
        for (Map.Entry<Text, byte[]> entry : client.getRow(new Text(getRow())).entrySet()) {

          byte[] value = entry.getValue();
          String cellData = new String(value);

          if (entry.getKey().equals(HConstants.COL_REGIONINFO)) {
            DataInputBuffer inbuf = new DataInputBuffer();
            HRegionInfo info = new HRegionInfo();
            inbuf.reset(value, value.length);
            info.readFields(inbuf);

            cellData = "ID : " + String.valueOf(info.getRegionId());
          }
          ConsoleTable.printLine(count, getRow().toString(), entry.getKey().toString(),
              cellData);
          count++;
        }
        ConsoleTable.selectFoot();

        break;

      case 2:

        Text[] column = new Text[] { new Text(getColumn()) };

        HScannerInterface scanner = client.obtainScanner(column, new Text(""));
        HStoreKey k = new HStoreKey();
        TreeMap<Text, byte[]> r = new TreeMap<Text, byte[]>();

        ConsoleTable.selectHead();
        count = 0;
        while (scanner.next(k, r)) {
          Text rowKey = k.getRow();

          for (Text columnKey : r.keySet()) {
            byte[] value = r.get(columnKey);
            String cellData = new String(value);
            ConsoleTable.printLine(count, rowKey.toString(), columnKey.toString(),
                cellData);
            count++;
          }
          results = new TreeMap<Text, byte[]>();
        }
        ConsoleTable.selectFoot();
        scanner.close();

        break;

      case 3:

        byte[] rs1 = client.get(new Text(getRow()), new Text(getColumn()));

        ConsoleTable.selectHead();
        ConsoleTable.printLine(0, getRow(), getColumn(),
          new String(rs1, HConstants.UTF8_ENCODING));
        ConsoleTable.selectFoot();

        break;

      case 4:

        byte[][] rs2 = client.get(new Text(getRow()), new Text(getColumn()), this.limit);

        ConsoleTable.selectHead();
        for (int i = 0; i < rs2.length; i++) {
          ConsoleTable.printLine(i, getRow(), getColumn(),
            new String(rs2[i], HConstants.UTF8_ENCODING));
        }
        ConsoleTable.selectFoot();

        break;

      case 5:

        byte[][] rs3 = client.get(new Text(getRow()), new Text(getColumn()), getTime(), this.limit);

        ConsoleTable.selectHead();
        for (int i = 0; i < rs3.length; i++) {
          ConsoleTable.printLine(i, getRow(), getColumn(), new String(rs3[i]));
        }
        ConsoleTable.selectFoot();

        break;

      }

      return new ReturnMsg(1, "Successfully print out the selected data.");
    } catch (IOException e) {
      String[] msg = e.getMessage().split("[,]");
      return new ReturnMsg(0, msg[0]);
    }
  }

  public void setTable(String table) {
    this.table = table;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setCondition(Map<String, List<String>> cond) {
    this.condition = cond;
  }

  public String getRow() {
    return this.condition.get("row").get(1);
  }

  public String getColumn() {
    return this.condition.get("column").get(1);
  }

  public long getTime() {
    return Long.parseLong(this.condition.get("time").get(1));
  }

  public int getConditionSize() {
    return this.condition.size();
  }

  public int getCondition() {
    int type = 0;
    if (this.condition == null) {
      type = 0;
    } else if (this.condition.containsKey("row")) {
      if (getConditionSize() == 1) {
        type = 1;
      } else if (this.condition.containsKey("column")) {
        if (getConditionSize() == 2) {
          if (this.limit == 0) {
            type = 3;
          } else {
            type = 4;
          }
        } else {
          type = 5;
        }
      }
    } else if (this.condition.containsKey("column")) {
      type = 2;
    }
    return type;
  }
}
