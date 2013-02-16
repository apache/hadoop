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


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
/*
Stub for  TestPipeApplication   test. This stub produced test data for main test. Main test  checks data
 */

public class PipeApplicationStub extends CommonStub {

  public static void main(String[] args) {
    PipeApplicationStub client = new PipeApplicationStub();
    client.binaryProtocolStub();
  }

  public void binaryProtocolStub() {
    try {

      initSoket();

      // output code
      WritableUtils.writeVInt(dataOut, 50);
      IntWritable wt = new IntWritable();
      wt.set(123);
      writeObject(wt, dataOut);
      writeObject(new Text("value"), dataOut);

      //  PARTITIONED_OUTPUT
      WritableUtils.writeVInt(dataOut, 51);
      WritableUtils.writeVInt(dataOut, 0);
      writeObject(wt, dataOut);
      writeObject(new Text("value"), dataOut);


      // STATUS
      WritableUtils.writeVInt(dataOut, 52);
      Text.writeString(dataOut, "PROGRESS");
      dataOut.flush();

      // progress
      WritableUtils.writeVInt(dataOut, 53);
      dataOut.writeFloat(0.55f);
      // register counter
      WritableUtils.writeVInt(dataOut, 55);
      // id
      WritableUtils.writeVInt(dataOut, 0);
      Text.writeString(dataOut, "group");
      Text.writeString(dataOut, "name");
      // increment counter
      WritableUtils.writeVInt(dataOut, 56);
      WritableUtils.writeVInt(dataOut, 0);

      WritableUtils.writeVLong(dataOut, 2);

      // map item
      int intValue = WritableUtils.readVInt(dataInput);
      System.out.println("intValue:" + intValue);
      IntWritable iw = new IntWritable();
      readObject(iw, dataInput);
      System.out.println("key:" + iw.get());
      Text txt = new Text();
      readObject(txt, dataInput);
      System.out.println("value:" + txt.toString());

      // done
      // end of session
      WritableUtils.writeVInt(dataOut, 54);

      System.out.println("finish");
      dataOut.flush();
      dataOut.close();

    } catch (Exception x) {
      x.printStackTrace();
    } finally {
      closeSoket();
    }
  }


}
