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
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.mapred.TextInputFormat;

public class PlanUtils {

  public static enum ExpressionTypes {FIELD, JEXL};

  @SuppressWarnings("nls")
  public static mapredWork getMapRedWork() {
    return new mapredWork("", 
                          new LinkedHashMap<String, ArrayList<String>> (),
                          new LinkedHashMap<String, partitionDesc> (),
                          new HashMap<String, Operator<? extends Serializable>> (),
                          new schemaDesc(),
                          new HashMap<String, schemaDesc> (),
                          null,
                          Integer.valueOf (1));
  }
  
  public static tableDesc getDefaultTableDesc(String separatorCode, String columns) {
    return new tableDesc(
        MetadataTypedColumnsetSerDe.class,
        TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class,
        Utilities.makeProperties(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, separatorCode,
            "columns", columns));    
  }
  public static tableDesc getDefaultTableDesc(String separatorCode) {
    return new tableDesc(
        MetadataTypedColumnsetSerDe.class,
        TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class,
        Utilities.makeProperties(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, separatorCode));    
  }

  
  // We will make reduce key and reduce value TableDesc with configurable SerDes   
  public static reduceSinkDesc getReduceSinkDesc(ArrayList<exprNodeDesc> keyCols, 
                                                 ArrayList<exprNodeDesc> valueCols, 
                                                 int tag, int numPartitionFields, 
                                                 int numReducers, boolean inferNumReducers) {
     
    return new reduceSinkDesc(keyCols, valueCols, tag, numPartitionFields, numReducers, inferNumReducers,
      getDefaultTableDesc("" + Utilities.ctrlaCode, ObjectInspectorUtils.getIntegerCSV(keyCols.size())),
      getDefaultTableDesc("" + Utilities.ctrlaCode, ObjectInspectorUtils.getIntegerCSV(valueCols.size())));
  }

  // We should read the TableDesc from gWork when it is available.   
  public static tableDesc getReduceKeyDesc(mapredWork gWork) {
     return getDefaultTableDesc("" + Utilities.ctrlaCode);
  }

  // We should read the TableDesc from gWork when it is available.   
  public static tableDesc getReduceValueDesc(mapredWork gWork, int tag) {
     return getDefaultTableDesc("" + Utilities.ctrlaCode);
  }
  
}
