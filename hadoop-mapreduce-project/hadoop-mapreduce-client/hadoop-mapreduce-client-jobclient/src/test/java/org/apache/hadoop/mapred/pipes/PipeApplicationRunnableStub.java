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

package org.apache.hadoop.mapred.pipes;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

/*
 Stub for  TestPipeApplication   test. This stub produced test data for main test. Main test  checks data
 */

public class PipeApplicationRunnableStub extends CommonStub {

  public static void main(String[] args) {
    PipeApplicationRunnableStub client = new PipeApplicationRunnableStub();
    client.binaryProtocolStub();
  }

  public void binaryProtocolStub() {
    try {

      initSoket();
      System.out.println("start OK");

      // RUN_MAP.code
      // should be 3

      int answer = WritableUtils.readVInt(dataInput);
      System.out.println("RunMap:" + answer);
      TestPipeApplication.FakeSplit split = new TestPipeApplication.FakeSplit();
      readObject(split, dataInput);

      WritableUtils.readVInt(dataInput);
      WritableUtils.readVInt(dataInput);
      // end runMap
      // get InputTypes
      WritableUtils.readVInt(dataInput);
      String inText = Text.readString(dataInput);
      System.out.println("Key class:" + inText);
      inText = Text.readString(dataInput);
      System.out.println("Value class:" + inText);

      @SuppressWarnings("unused")
      int inCode = 0;

      // read all data from sender and write to output
      while ((inCode = WritableUtils.readVInt(dataInput)) == 4) {
        FloatWritable key = new FloatWritable();
        NullWritable value = NullWritable.get();
        readObject(key, dataInput);
        System.out.println("value:" + key.get());
        readObject(value, dataInput);
      }

      WritableUtils.writeVInt(dataOut, 54);

      dataOut.flush();
      dataOut.close();

    } catch (Exception x) {
      x.printStackTrace();
    } finally {
      closeSoket();
    }

  }

}
