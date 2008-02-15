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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.BloomFilterDescriptor;
import org.apache.hadoop.hbase.BloomFilterDescriptor.BloomFilterType;

/**
 * Alters tables.
 */
public class AlterCommand extends SchemaModificationCommand {
  public enum OperationType {
    ADD, DROP, CHANGE, NOOP
  }

  private OperationType operationType = OperationType.NOOP;
  private Map<String, Map<String, Object>> columnSpecMap = new HashMap<String, Map<String, Object>>();
  private String tableName;
  private String column; // column to be dropped

  public AlterCommand(Writer o) {
    super(o);
  }

  @SuppressWarnings("unchecked")
  public ReturnMsg execute(HBaseConfiguration conf) {
    try {
      HConnection conn = HConnectionManager.getConnection(conf);
      if (!conn.tableExists(new Text(this.tableName))) {
        return new ReturnMsg(0, "'" + this.tableName + "'" + TABLE_NOT_FOUND);
      }

      HBaseAdmin admin = new HBaseAdmin(conf);
      Set<String> columns = null;
      HColumnDescriptor columnDesc = null;
      switch (operationType) {
        case ADD:
          disableTable(admin, tableName);
          columns = columnSpecMap.keySet();
          for (String c : columns) {
            columnDesc = getColumnDescriptor(c, columnSpecMap.get(c));
            println("Adding " + c + " to " + tableName + "... Please wait.");
            admin.addColumn(new Text(tableName), columnDesc);
          }
          enableTable(admin, tableName);
          break;
        case DROP:
          disableTable(admin, tableName);
          println("Dropping " + column + " from " + tableName + "... Please wait.");
          column = appendDelimiter(column);
          admin.deleteColumn(new Text(tableName), new Text(column));
          enableTable(admin, tableName);
          break;
        case CHANGE:
          disableTable(admin, tableName);

          Map.Entry<String, Map<String, Object>> columnEntry = (Map.Entry<String, Map<String, Object>>) columnSpecMap
              .entrySet().toArray()[0];

          // add the : if there isn't one
          Text columnName = new Text(
              columnEntry.getKey().endsWith(":") ? columnEntry.getKey()
                  : columnEntry.getKey() + ":");

          // get the table descriptor so we can get the old column descriptor
          HTableDescriptor tDesc = getTableDescByName(admin, tableName);
          HColumnDescriptor oldColumnDesc = tDesc.families().get(columnName);

          // combine the options specified in the shell with the options
          // from the exiting descriptor to produce the new descriptor
          columnDesc = getColumnDescriptor(columnName.toString(), columnEntry
              .getValue(), oldColumnDesc);

          // send the changes out to the master
          admin.modifyColumn(new Text(tableName), columnName, columnDesc);

          enableTable(admin, tableName);
          break;
        case NOOP:
          return new ReturnMsg(0, "Invalid operation type.");
      }
      return new ReturnMsg(0, "Table altered successfully.");
    } catch (Exception e) {
      return new ReturnMsg(0, extractErrMsg(e));
    }
  }

  private void disableTable(HBaseAdmin admin, String t) throws IOException {
    println("Disabling " + t + "... Please wait.");
    admin.disableTable(new Text(t));
  }

  private void enableTable(HBaseAdmin admin, String t) throws IOException {
    println("Enabling " + t + "... Please wait.");
    admin.enableTable(new Text(t));
  }

  /**
   * Sets the table to be altered.
   * 
   * @param t Table to be altered.
   */
  public void setTable(String t) {
    this.tableName = t;
  }

  /**
   * Adds a column specification.
   * 
   * @param columnSpec Column specification
   */
  public void addColumnSpec(String c, Map<String, Object> columnSpec) {
    columnSpecMap.put(c, columnSpec);
  }

  /**
   * Sets the column to be dropped. Only applicable to the DROP operation.
   * 
   * @param c Column to be dropped.
   */
  public void setColumn(String c) {
    this.column = c;
  }

  /**
   * Sets the operation type of this alteration.
   * 
   * @param operationType Operation type
   * @see OperationType
   */
  public void setOperationType(OperationType operationType) {
    this.operationType = operationType;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.DDL;
  }

  private HTableDescriptor getTableDescByName(HBaseAdmin admin, String tableName)
      throws IOException {
    HTableDescriptor[] tables = admin.listTables();
    for (HTableDescriptor tDesc : tables) {
      if (tDesc.getName().toString().equals(tableName)) {
        return tDesc;
      }
    }
    return null;
  }

  /**
   * Given a column name, column spec, and original descriptor, returns an
   * instance of HColumnDescriptor representing the column spec, with empty
   * values drawn from the original as defaults
   */
  protected HColumnDescriptor getColumnDescriptor(String column,
      Map<String, Object> columnSpec, HColumnDescriptor original)
      throws IllegalArgumentException {
    initOptions(original);

    Set<String> specs = columnSpec.keySet();
    for (String spec : specs) {
      spec = spec.toUpperCase();

      if (spec.equals("MAX_VERSIONS")) {
        maxVersions = (Integer) columnSpec.get(spec);
      } else if (spec.equals("MAX_LENGTH")) {
        maxLength = (Integer) columnSpec.get(spec);
      } else if (spec.equals("COMPRESSION")) {
        compression = HColumnDescriptor.CompressionType.valueOf(((String) columnSpec
            .get(spec)).toUpperCase());
      } else if (spec.equals("IN_MEMORY")) {
        inMemory = (Boolean) columnSpec.get(spec);
      } else if (spec.equals("BLOCK_CACHE_ENABLED")) {
        blockCacheEnabled = (Boolean) columnSpec.get(spec);
      } else if (spec.equals("BLOOMFILTER")) {
        bloomFilterType = BloomFilterType.valueOf(((String) columnSpec.get(spec))
            .toUpperCase());
      } else if (spec.equals("VECTOR_SIZE")) {
        vectorSize = (Integer) columnSpec.get(spec);
      } else if (spec.equals("NUM_HASH")) {
        numHash = (Integer) columnSpec.get(spec);
      } else if (spec.equals("NUM_ENTRIES")) {
        numEntries = (Integer) columnSpec.get(spec);
      } else {
        throw new IllegalArgumentException("Invalid option: " + spec);
      }
    }

    // Now we gather all the specified options for this column.
    if (bloomFilterType != null) {
      if (specs.contains("NUM_ENTRIES")) {
        bloomFilterDesc = new BloomFilterDescriptor(bloomFilterType, numEntries);
      } else {
        bloomFilterDesc = new BloomFilterDescriptor(bloomFilterType, vectorSize,
            numHash);
      }
    }

    column = appendDelimiter(column);

    HColumnDescriptor columnDesc = new HColumnDescriptor(new Text(column),
        maxVersions, compression, inMemory, blockCacheEnabled,
        maxLength, bloomFilterDesc);

    return columnDesc;
  }

  private void initOptions(HColumnDescriptor original) {
    if (original == null) {
      initOptions();
      return;
    }
    maxVersions = original.getMaxVersions();
    maxLength = original.getMaxValueLength();
    compression = original.getCompression();
    inMemory = original.isInMemory();
    blockCacheEnabled = original.isBlockCacheEnabled();
    bloomFilterDesc = original.getBloomFilter();
  }
}
