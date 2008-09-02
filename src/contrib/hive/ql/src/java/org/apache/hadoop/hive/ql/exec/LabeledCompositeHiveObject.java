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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.serde.*;
/**
 * wrapper over composite hive object that attaches names to each field
 * (instead of the positional names of CompositeHiveObject)
 */
public class LabeledCompositeHiveObject extends CompositeHiveObject {
  String [] labels;

  public LabeledCompositeHiveObject(int width) {
    super(width);
    throw new RuntimeException ("Labaled Hive Objects require field names");
  }

  public LabeledCompositeHiveObject(String [] labels) {
    super(labels.length);
    this.labels = labels;
  }

  @Override
  public SerDeField getFieldFromExpression(String expr) {

    int dot = expr.indexOf(".");
    String label = expr;
    if(dot != -1) {
      assert(dot != (expr.length()-1));

      label = expr.substring(0, dot);
      expr =  expr.substring(dot+1);
    } else {
      expr = null;
    }

    for(int i=0; i<width; i++) {
      if(label.equals(labels[i])) {
        return new CompositeSerDeField(i, expr);
      }
    }
    throw new RuntimeException ("Cannot match expression "+label+"."+expr+" against any label!");
  }
}
