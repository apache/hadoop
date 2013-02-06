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


import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

/*
Stub for  TestPipeApplication   test. This stub produced test data for main test. Main test  checks data
 */

public class PipeReducerStub extends CommonStub {

  public static void main(String[] args) {
    PipeReducerStub client = new PipeReducerStub();
    client.binaryProtocolStub();
  }

  public void binaryProtocolStub() {
    try {

      initSoket();

      //should be 5
      //RUN_REDUCE boolean 
      WritableUtils.readVInt(dataInput);
      WritableUtils.readVInt(dataInput);
      int intValue = WritableUtils.readVInt(dataInput);
      System.out.println("getIsJavaRecordWriter:" + intValue);

      // reduce key
      WritableUtils.readVInt(dataInput);
      // value of reduce key
      BooleanWritable value = new BooleanWritable();
      readObject(value, dataInput);
      System.out.println("reducer key :" + value);
      // reduce value code:

      // reduce values
      while ((intValue = WritableUtils.readVInt(dataInput)) == 7) {
        Text txt = new Text();
        // value
        readObject(txt, dataInput);
        System.out.println("reduce value  :" + txt);
      }


      // done
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
