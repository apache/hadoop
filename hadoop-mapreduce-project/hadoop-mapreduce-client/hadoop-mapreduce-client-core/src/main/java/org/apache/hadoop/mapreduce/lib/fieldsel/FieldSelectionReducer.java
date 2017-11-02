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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a reducer class that can be used to perform field
 * selections in a manner similar to unix cut. 
 * 
 * The input data is treated as fields separated by a user specified
 * separator (the default value is "\t"). The user can specify a list of
 * fields that form the reduce output keys, and a list of fields that form
 * the reduce output values. The fields are the union of those from the key
 * and those from the value.
 * 
 * The field separator is under attribute "mapreduce.fieldsel.data.field.separator"
 * 
 * The reduce output field list spec is under attribute 
 * "mapreduce.fieldsel.reduce.output.key.value.fields.spec". 
 * The value is expected to be like
 * "keyFieldsSpec:valueFieldsSpec" key/valueFieldsSpec are comma (,) 
 * separated field spec: fieldSpec,fieldSpec,fieldSpec ... Each field spec
 * can be a simple number (e.g. 5) specifying a specific field, or a range
 * (like 2-5) to specify a range of fields, or an open range (like 3-) 
 * specifying all the fields starting from field 3. The open range field
 * spec applies value fields only. They have no effect on the key fields.
 * 
 * Here is an example: "4,3,0,1:6,5,1-3,7-". It specifies to use fields
 * 4,3,0 and 1 for keys, and use fields 6,5,1,2,3,7 and above for values.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FieldSelectionReducer<K, V>
    extends Reducer<Text, Text, Text, Text> {

  private String fieldSeparator = "\t";

  private String reduceOutputKeyValueSpec;

  private List<Integer> reduceOutputKeyFieldList = new ArrayList<Integer>();

  private List<Integer> reduceOutputValueFieldList = new ArrayList<Integer>();

  private int allReduceValueFieldsFrom = -1;

  public static final Logger LOG =
      LoggerFactory.getLogger("FieldSelectionMapReduce");

  public void setup(Context context) 
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    
    this.fieldSeparator = 
      conf.get(FieldSelectionHelper.DATA_FIELD_SEPARATOR, "\t");
    
    this.reduceOutputKeyValueSpec = 
      conf.get(FieldSelectionHelper.REDUCE_OUTPUT_KEY_VALUE_SPEC, "0-:");
    
    allReduceValueFieldsFrom = FieldSelectionHelper.parseOutputKeyValueSpec(
      reduceOutputKeyValueSpec, reduceOutputKeyFieldList,
      reduceOutputValueFieldList);

    LOG.info(FieldSelectionHelper.specToString(fieldSeparator,
      reduceOutputKeyValueSpec, allReduceValueFieldsFrom,
      reduceOutputKeyFieldList, reduceOutputValueFieldList));
  }

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    String keyStr = key.toString() + this.fieldSeparator;
    
    for (Text val : values) {
      FieldSelectionHelper helper = new FieldSelectionHelper();
      helper.extractOutputKeyValue(keyStr, val.toString(),
        fieldSeparator, reduceOutputKeyFieldList,
        reduceOutputValueFieldList, allReduceValueFieldsFrom, false, false);
      context.write(helper.getKey(), helper.getValue());
    }
  }
}
