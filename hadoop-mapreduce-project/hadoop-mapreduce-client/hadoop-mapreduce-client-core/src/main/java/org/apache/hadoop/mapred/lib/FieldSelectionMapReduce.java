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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.fieldsel.*;

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
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FieldSelectionMapReduce<K, V>
    implements Mapper<K, V, Text, Text>, Reducer<Text, Text, Text, Text> {

  private String mapOutputKeyValueSpec;

  private boolean ignoreInputKey;

  private String fieldSeparator = "\t";

  private List<Integer> mapOutputKeyFieldList = new ArrayList<Integer>();

  private List<Integer> mapOutputValueFieldList = new ArrayList<Integer>();

  private int allMapValueFieldsFrom = -1;

  private String reduceOutputKeyValueSpec;

  private List<Integer> reduceOutputKeyFieldList = new ArrayList<Integer>();

  private List<Integer> reduceOutputValueFieldList = new ArrayList<Integer>();

  private int allReduceValueFieldsFrom = -1;


  public static final Log LOG = LogFactory.getLog("FieldSelectionMapReduce");

  private String specToString() {
    StringBuffer sb = new StringBuffer();
    sb.append("fieldSeparator: ").append(fieldSeparator).append("\n");

    sb.append("mapOutputKeyValueSpec: ").append(mapOutputKeyValueSpec).append(
        "\n");
    sb.append("reduceOutputKeyValueSpec: ").append(reduceOutputKeyValueSpec)
        .append("\n");

    sb.append("allMapValueFieldsFrom: ").append(allMapValueFieldsFrom).append(
        "\n");

    sb.append("allReduceValueFieldsFrom: ").append(allReduceValueFieldsFrom)
        .append("\n");

    int i = 0;

    sb.append("mapOutputKeyFieldList.length: ").append(
        mapOutputKeyFieldList.size()).append("\n");
    for (i = 0; i < mapOutputKeyFieldList.size(); i++) {
      sb.append("\t").append(mapOutputKeyFieldList.get(i)).append("\n");
    }
    sb.append("mapOutputValueFieldList.length: ").append(
        mapOutputValueFieldList.size()).append("\n");
    for (i = 0; i < mapOutputValueFieldList.size(); i++) {
      sb.append("\t").append(mapOutputValueFieldList.get(i)).append("\n");
    }

    sb.append("reduceOutputKeyFieldList.length: ").append(
        reduceOutputKeyFieldList.size()).append("\n");
    for (i = 0; i < reduceOutputKeyFieldList.size(); i++) {
      sb.append("\t").append(reduceOutputKeyFieldList.get(i)).append("\n");
    }
    sb.append("reduceOutputValueFieldList.length: ").append(
        reduceOutputValueFieldList.size()).append("\n");
    for (i = 0; i < reduceOutputValueFieldList.size(); i++) {
      sb.append("\t").append(reduceOutputValueFieldList.get(i)).append("\n");
    }
    return sb.toString();
  }

  /**
   * The identify function. Input key/value pair is written directly to output.
   */
  public void map(K key, V val,
      OutputCollector<Text, Text> output, Reporter reporter) 
      throws IOException {
    FieldSelectionHelper helper = new FieldSelectionHelper(
      FieldSelectionHelper.emptyText, FieldSelectionHelper.emptyText);
    helper.extractOutputKeyValue(key.toString(), val.toString(),
      fieldSeparator, mapOutputKeyFieldList, mapOutputValueFieldList,
      allMapValueFieldsFrom, ignoreInputKey, true);
    output.collect(helper.getKey(), helper.getValue());
  }

  private void parseOutputKeyValueSpec() {
    allMapValueFieldsFrom = FieldSelectionHelper.parseOutputKeyValueSpec(
      mapOutputKeyValueSpec, mapOutputKeyFieldList, mapOutputValueFieldList);
    
    allReduceValueFieldsFrom = FieldSelectionHelper.parseOutputKeyValueSpec(
      reduceOutputKeyValueSpec, reduceOutputKeyFieldList,
      reduceOutputValueFieldList);
  }

  public void configure(JobConf job) {
    this.fieldSeparator = job.get(FieldSelectionHelper.DATA_FIELD_SEPERATOR,
        "\t");
    this.mapOutputKeyValueSpec = job.get(
        FieldSelectionHelper.MAP_OUTPUT_KEY_VALUE_SPEC, "0-:");
    this.ignoreInputKey = TextInputFormat.class.getCanonicalName().equals(
        job.getInputFormat().getClass().getCanonicalName());
    this.reduceOutputKeyValueSpec = job.get(
        FieldSelectionHelper.REDUCE_OUTPUT_KEY_VALUE_SPEC, "0-:");
    parseOutputKeyValueSpec();
    LOG.info(specToString());
  }

  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  public void reduce(Text key, Iterator<Text> values,
                     OutputCollector<Text, Text> output, Reporter reporter)
    throws IOException {
    String keyStr = key.toString() + this.fieldSeparator;
    while (values.hasNext()) {
        FieldSelectionHelper helper = new FieldSelectionHelper();
        helper.extractOutputKeyValue(keyStr, values.next().toString(),
          fieldSeparator, reduceOutputKeyFieldList,
          reduceOutputValueFieldList, allReduceValueFieldsFrom, false, false);
      output.collect(helper.getKey(), helper.getValue());
    }
  }
}
