/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Response class for MultiPut.
 */
public class MultiPutResponse implements Writable {

  protected MultiPut request; // used in client code ONLY

  protected Map<byte[], Integer> answers = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);

  public MultiPutResponse() {}

  public void addResult(byte[] regionName, int result) {
    answers.put(regionName, result);
  }

  public Integer getAnswer(byte[] region) {
    return answers.get(region);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(answers.size());
    for( Map.Entry<byte[],Integer> e : answers.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());
      out.writeInt(e.getValue());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    answers.clear();

    int mapSize = in.readInt();
    for( int i = 0 ; i < mapSize ; i++ ) {
      byte[] key = Bytes.readByteArray(in);
      int value = in.readInt();

      answers.put(key, value);
    }
  }
}
