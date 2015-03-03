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

package org.apache.hadoop.mapreduce.lib.fieldsel;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;

/**
 * This class implements a mapper/reducer class that can be used to perform
 * field selections in a manner similar to unix cut. The input data is treated
 * as fields separated by a user specified separator (the default value is
 * "\t"). The user can specify a list of fields that form the map output keys,
 * and a list of fields that form the map output values. If the inputformat is
 * TextInputFormat, the mapper will ignore the key to the map function. and the
 * fields are from the value only. Otherwise, the fields are the union of those
 * from the key and those from the value.
 * 
 * The field separator is under attribute "mapreduce.fieldsel.data.field.separator"
 * 
 * The map output field list spec is under attribute 
 * "mapreduce.fieldsel.map.output.key.value.fields.spec".
 * The value is expected to be like "keyFieldsSpec:valueFieldsSpec"
 * key/valueFieldsSpec are comma (,) separated field spec: fieldSpec,fieldSpec,fieldSpec ...
 * Each field spec can be a simple number (e.g. 5) specifying a specific field, or a range
 * (like 2-5) to specify a range of fields, or an open range (like 3-) specifying all 
 * the fields starting from field 3. The open range field spec applies value fields only.
 * They have no effect on the key fields.
 * 
 * Here is an example: "4,3,0,1:6,5,1-3,7-". It specifies to use fields 4,3,0 and 1 for keys,
 * and use fields 6,5,1,2,3,7 and above for values.
 * 
 * The reduce output field list spec is under attribute 
 * "mapreduce.fieldsel.reduce.output.key.value.fields.spec".
 * 
 * The reducer extracts output key/value pairs in a similar manner, except that
 * the key is never ignored.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FieldSelectionHelper {

  public static Text emptyText = new Text("");
  public static final String DATA_FIELD_SEPERATOR = 
    "mapreduce.fieldsel.data.field.separator";
  public static final String MAP_OUTPUT_KEY_VALUE_SPEC = 
    "mapreduce.fieldsel.map.output.key.value.fields.spec";
  public static final String REDUCE_OUTPUT_KEY_VALUE_SPEC = 
    "mapreduce.fieldsel.reduce.output.key.value.fields.spec";


  /**
   * Extract the actual field numbers from the given field specs.
   * If a field spec is in the form of "n-" (like 3-), then n will be the 
   * return value. Otherwise, -1 will be returned.  
   * @param fieldListSpec an array of field specs
   * @param fieldList an array of field numbers extracted from the specs.
   * @return number n if some field spec is in the form of "n-", -1 otherwise.
   */
  private static int extractFields(String[] fieldListSpec,
      List<Integer> fieldList) {
    int allFieldsFrom = -1;
    int i = 0;
    int j = 0;
    int pos = -1;
    String fieldSpec = null;
    for (i = 0; i < fieldListSpec.length; i++) {
      fieldSpec = fieldListSpec[i];
      if (fieldSpec.length() == 0) {
        continue;
      }
      pos = fieldSpec.indexOf('-');
      if (pos < 0) {
        Integer fn = Integer.valueOf(fieldSpec);
        fieldList.add(fn);
      } else {
        String start = fieldSpec.substring(0, pos);
        String end = fieldSpec.substring(pos + 1);
        if (start.length() == 0) {
          start = "0";
        }
        if (end.length() == 0) {
          allFieldsFrom = Integer.parseInt(start);
          continue;
        }
        int startPos = Integer.parseInt(start);
        int endPos = Integer.parseInt(end);
        for (j = startPos; j <= endPos; j++) {
          fieldList.add(j);
        }
      }
    }
    return allFieldsFrom;
  }

  private static String selectFields(String[] fields, List<Integer> fieldList,
      int allFieldsFrom, String separator) {
    String retv = null;
    int i = 0;
    StringBuffer sb = null;
    if (fieldList != null && fieldList.size() > 0) {
      if (sb == null) {
        sb = new StringBuffer();
      }
      for (Integer index : fieldList) {
        if (index < fields.length) {
          sb.append(fields[index]);
        }
        sb.append(separator);
      }
    }
    if (allFieldsFrom >= 0) {
      if (sb == null) {
        sb = new StringBuffer();
      }
      for (i = allFieldsFrom; i < fields.length; i++) {
        sb.append(fields[i]).append(separator);
      }
    }
    if (sb != null) {
      retv = sb.toString();
      if (retv.length() > 0) {
        retv = retv.substring(0, retv.length() - 1);
      }
    }
    return retv;
  }
  
  public static int parseOutputKeyValueSpec(String keyValueSpec,
      List<Integer> keyFieldList, List<Integer> valueFieldList) {
    String[] keyValSpecs = keyValueSpec.split(":", -1);
    
    String[] keySpec = keyValSpecs[0].split(",");
    
    String[] valSpec = new String[0];
    if (keyValSpecs.length > 1) {
      valSpec = keyValSpecs[1].split(",");
    }

    FieldSelectionHelper.extractFields(keySpec, keyFieldList);
    return FieldSelectionHelper.extractFields(valSpec, valueFieldList);
  }

  public static String specToString(String fieldSeparator, String keyValueSpec,
      int allValueFieldsFrom, List<Integer> keyFieldList,
      List<Integer> valueFieldList) {
    StringBuffer sb = new StringBuffer();
    sb.append("fieldSeparator: ").append(fieldSeparator).append("\n");

    sb.append("keyValueSpec: ").append(keyValueSpec).append("\n");
    sb.append("allValueFieldsFrom: ").append(allValueFieldsFrom);
    sb.append("\n");
    sb.append("keyFieldList.length: ").append(keyFieldList.size());
    sb.append("\n");
    for (Integer field : keyFieldList) {
      sb.append("\t").append(field).append("\n");
    }
    sb.append("valueFieldList.length: ").append(valueFieldList.size());
    sb.append("\n");
    for (Integer field : valueFieldList) {
      sb.append("\t").append(field).append("\n");
    }
    return sb.toString();
  }

  private Text key = null;
  private Text value = null;
  
  public FieldSelectionHelper() {
  }

  public FieldSelectionHelper(Text key, Text val) {
    this.key = key;
    this.value = val;
  }
  
  public Text getKey() {
    return key;
  }
 
  public Text getValue() {
    return value;
  }

  public void extractOutputKeyValue(String key, String val,
      String fieldSep, List<Integer> keyFieldList, List<Integer> valFieldList,
      int allValueFieldsFrom, boolean ignoreKey, boolean isMap) {
    if (!ignoreKey) {
      val = key + val;
    }
    String[] fields = val.split(fieldSep);
    
    String newKey = selectFields(fields, keyFieldList, -1, fieldSep);
    String newVal = selectFields(fields, valFieldList, allValueFieldsFrom,
      fieldSep);
    if (isMap && newKey == null) {
      newKey = newVal;
      newVal = null;
    }
    
    if (newKey != null) {
      this.key = new Text(newKey);
    }
    if (newVal != null) {
      this.value = new Text(newVal);
    }
  }
}
