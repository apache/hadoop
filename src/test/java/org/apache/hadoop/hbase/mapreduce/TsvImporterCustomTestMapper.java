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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;

/**
 * Dummy mapper used for unit tests to verify that the mapper can be injected.
 * This approach would be used if a custom transformation needed to be done after
 * reading the input data before writing it to HFiles.
 */
public class TsvImporterCustomTestMapper extends TsvImporterMapper {

  @Override
  protected void setup(Context context) {
    doSetup(context);
  }

  /**
   * Convert a line of TSV text into an HBase table row after transforming the
   * values by multiplying them by 3.
   */
  @Override
  public void map(LongWritable offset, Text value, Context context)
        throws IOException {
    byte[] family = Bytes.toBytes("FAM");
    final byte[][] qualifiers = { Bytes.toBytes("A"), Bytes.toBytes("B") };

    // do some basic line parsing
    byte[] lineBytes = value.getBytes();
    String[] valueTokens = new String(lineBytes, "UTF-8").split("\u001b");

    // create the rowKey and Put
    ImmutableBytesWritable rowKey =
      new ImmutableBytesWritable(Bytes.toBytes(valueTokens[0]));
    Put put = new Put(rowKey.copyBytes());

    //The value should look like this: VALUE1 or VALUE2. Let's multiply
    //the integer by 3
    for(int i = 1; i < valueTokens.length; i++) {
      String prefix = valueTokens[i].substring(0, "VALUE".length());
      String suffix = valueTokens[i].substring("VALUE".length());
      String newValue = prefix + Integer.parseInt(suffix) * 3;

      KeyValue kv = new KeyValue(rowKey.copyBytes(), family,
          qualifiers[i-1], Bytes.toBytes(newValue));
      put.add(kv);
    }

    try {
      context.write(rowKey, put);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
