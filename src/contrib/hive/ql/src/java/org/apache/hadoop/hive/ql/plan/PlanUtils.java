/**
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

package org.apache.hadoop.hive.ql.plan;

import java.util.*;
import java.io.*;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.thrift.TBinarySortableProtocol;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import com.facebook.thrift.protocol.TBinaryProtocol;

public class PlanUtils {

  public static enum ExpressionTypes {FIELD, JEXL};

  @SuppressWarnings("nls")
  public static mapredWork getMapRedWork() {
    return new mapredWork("", 
                          new LinkedHashMap<String, ArrayList<String>> (),
                          new LinkedHashMap<String, partitionDesc> (),
                          new HashMap<String, Operator<? extends Serializable>> (),
                          new tableDesc(),
                          new ArrayList<tableDesc> (),
                          null,
                          Integer.valueOf (1));
  }
  
  /** 
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the separatorCode
   * and column names (comma separated string).
   */
  public static tableDesc getDefaultTableDesc(String separatorCode, String columns) {
    return getDefaultTableDesc(separatorCode, columns, false);
  }

  /** 
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the separatorCode
   * and column names (comma separated string), and whether the last column should take
   * the rest of the line.
   */
  public static tableDesc getDefaultTableDesc(String separatorCode, String columns,
      boolean lastColumnTakesRestOfTheLine) {
    Properties properties = Utilities.makeProperties(
        Constants.SERIALIZATION_FORMAT, separatorCode,
        "columns", columns);
    if (lastColumnTakesRestOfTheLine) {
      properties.setProperty(
          Constants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
          "true");
    }
    return new tableDesc(
        MetadataTypedColumnsetSerDe.class,
        TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class,
        properties);    
  }

  /** 
   * Generate the table descriptor of MetadataTypedColumnsetSerDe with the separatorCode
   */
  public static tableDesc getDefaultTableDesc(String separatorCode) {
    return new tableDesc(
        MetadataTypedColumnsetSerDe.class,
        TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class,
        Utilities.makeProperties(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, separatorCode));    
  }

  /** 
   * Generate the table descriptor of DynamicSerDe and TBinarySortableProtocol.
   */
  public static tableDesc getBinarySortableTableDesc(List<FieldSchema> fieldSchemas) {
    String structName = "binary_sortable_table";
    return new tableDesc(
        DynamicSerDe.class,
        SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class,
        Utilities.makeProperties(
            "name", structName,        
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, TBinarySortableProtocol.class.getName(),
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_DDL, 
              MetaStoreUtils.getDDLFromFieldSchema(structName, fieldSchemas)
        ));    
  }

  /** 
   * Generate the table descriptor of DynamicSerDe and TBinaryProtocol.
   */
  public static tableDesc getBinaryTableDesc(List<FieldSchema> fieldSchemas) {
    String structName = "binary_table";
    return new tableDesc(
        DynamicSerDe.class,
        SequenceFileInputFormat.class,
        SequenceFileOutputFormat.class,
        Utilities.makeProperties(
            "name", structName,
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, TBinaryProtocol.class.getName(),
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_DDL, 
              MetaStoreUtils.getDDLFromFieldSchema(structName, fieldSchemas)
        ));    
  }
  
  
  /** 
   * Convert the ColumnList to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnList(ArrayList<exprNodeDesc> cols, 
      String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i=0; i<cols.size(); i++) {
      schemas.add(TypeInfoUtils.getFieldSchemaFromTypeInfo(fieldPrefix + i, cols.get(i).getTypeInfo()));
    }
    return schemas;
  }
  
  /** 
   * Convert the RowSchema to FieldSchema list.
   */
  public static List<FieldSchema> getFieldSchemasFromRowSchema(RowSchema row, String fieldPrefix) {
    Vector<ColumnInfo> c = row.getSignature();
    return getFieldSchemasFromColumnInfo(c, fieldPrefix);
  }
  
  /** 
   * Convert the ColumnInfo to FieldSchema.
   */
  public static List<FieldSchema> getFieldSchemasFromColumnInfo(Vector<ColumnInfo> cols, String fieldPrefix) {
    List<FieldSchema> schemas = new ArrayList<FieldSchema>(cols.size());
    for (int i=0; i<cols.size(); i++) {
      String name = cols.get(i).getInternalName();
      if (name.equals(Integer.valueOf(i).toString())) {
        name = fieldPrefix + name; 
      }
      schemas.add(TypeInfoUtils.getFieldSchemaFromTypeInfo(name, cols.get(i).getType()));
    }
    return schemas;
  }
  
  /**
   * Create the reduce sink descriptor.
   * @param keyCols   The columns to be stored in the key
   * @param valueCols The columns to be stored in the value
   * @param tag       The tag for this reducesink
   * @param partitionCols The columns for partitioning.
   * @param numReducers  The number of reducers.
   * @param inferNumReducers  whether we should try to infer the number of reducers.
   * @return The reduceSinkDesc object.
   */
  public static reduceSinkDesc getReduceSinkDesc(ArrayList<exprNodeDesc> keyCols, 
                                                 ArrayList<exprNodeDesc> valueCols, 
                                                 int tag, 
                                                 ArrayList<exprNodeDesc> partitionCols, 
                                                 int numReducers, boolean inferNumReducers) {
    
    return new reduceSinkDesc(keyCols, valueCols, tag, partitionCols, numReducers, inferNumReducers,
        getBinarySortableTableDesc(getFieldSchemasFromColumnList(keyCols, "reducesinkkey")),
        getBinaryTableDesc(getFieldSchemasFromColumnList(valueCols, "reducesinkvalue")));
  }

  /**
   * Create the reduce sink descriptor.
   * @param keyCols   The columns to be stored in the key
   * @param valueCols The columns to be stored in the value
   * @param tag       The tag for this reducesink
   * @param numPartitionFields  The first numPartitionFields of keyCols will be partition columns.
   *                  If numPartitionFields=-1, then partition randomly.
   * @param numReducers  The number of reducers.
   * @param inferNumReducers  whether we should try to infer the number of reducers.
   * @return The reduceSinkDesc object.
   */
  public static reduceSinkDesc getReduceSinkDesc(ArrayList<exprNodeDesc> keyCols, 
                                                 ArrayList<exprNodeDesc> valueCols, 
                                                 int tag, 
                                                 int numPartitionFields, 
                                                 int numReducers, boolean inferNumReducers) {
    ArrayList<exprNodeDesc> partitionCols = null;

    if (numPartitionFields >= keyCols.size()) {
      partitionCols = keyCols;
    } else if (numPartitionFields >= 0) {
      partitionCols = new ArrayList<exprNodeDesc>(numPartitionFields);
      for (int i=0; i<numPartitionFields; i++) {
        partitionCols.add(keyCols.get(i));
      }
    } else {
      // numPartitionFields = -1 means random partitioning
      partitionCols = new ArrayList<exprNodeDesc>(1);
      partitionCols.add(SemanticAnalyzer.getFuncExprNodeDesc("rand"));
    }
    
    return getReduceSinkDesc(keyCols, valueCols, tag, partitionCols, numReducers, inferNumReducers);
  }
  
  
}
  