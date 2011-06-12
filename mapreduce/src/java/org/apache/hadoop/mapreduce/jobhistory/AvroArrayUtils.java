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

package org.apache.hadoop.mapreduce.jobhistory;

import java.lang.Integer;
import java.util.Iterator;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;

public class AvroArrayUtils {

  private static final Schema ARRAY_INT
      = Schema.createArray(Schema.create(Schema.Type.INT));

  static public GenericArray<Integer> NULL_PROGRESS_SPLITS_ARRAY
    = new GenericData.Array<Integer>(0, ARRAY_INT);

  public static GenericArray<Integer>
    toAvro(int values[]) {
    GenericData.Array<Integer> result
      = new GenericData.Array<Integer>(values.length, ARRAY_INT);

    for (int i = 0; i < values.length; ++i) {
      result.add(values[i]);
    }

    return result;
  }

  public static int[] fromAvro(java.util.List<Integer> avro) {
    int[] result = new int[(int)avro.size()];

    int i = 0;
      
    for (Iterator<Integer> iter = avro.iterator(); iter.hasNext(); ++i) {
      result[i] = iter.next();
    }

    return result;
  }
}
