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

import junit.framework.TestCase;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.utils.ByteStream;

public class TestWritables extends TestCase {

  protected CompositeHiveObject [] r;

  protected void setUp() {
    try {
      r = new CompositeHiveObject [5];
      for(int i=0; i<5; i++) {

        r[i] = new CompositeHiveObject (3);
        for (int j=0; j < 3; j++) {
          r[i].addHiveObject(new PrimitiveHiveObject(Integer.valueOf (i-1+j)));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException (e);
    }
  }

  public void testWritable() throws Exception {
    try {
      ByteStream.Output bos = new ByteStream.Output ();
      DataOutputStream dos = new DataOutputStream (bos);
      HiveObjectSerializer hos = new NaiiveSerializer();
      WritableHiveObject who = null;
      WritableHiveObject.setSerialFormat();
      for(int i=0; i < 5; i++) {
        who = new WritableHiveObject(i, null, hos);
        who.setHo(r[i]);
        who.write(dos);
      }

      ByteStream.Input bin = new ByteStream.Input(bos.getData(), 0, bos.getCount());
      DataInputStream din = new DataInputStream(bin);
      for(int i=0; i < 5; i++) {
        who.readFields(din);
        HiveObject ho = who.getHo();
        for(int j=0; j<3; j++) {
          SerDeField sdf = ho.getFieldFromExpression(""+j);
          String str = (String)ho.get(sdf).getJavaObject();
          assertEquals(str, new String(""+(i-1+j)));
        }
        assertEquals(who.getTag(), i);
      }
      System.out.println("testWritable OK");
    } catch (Exception e) {
      e.printStackTrace();
      throw (e);
    }
  }


  public void testNoTagWritable() throws Exception {

    try {
      ByteStream.Output bos = new ByteStream.Output ();
      DataOutputStream dos = new DataOutputStream (bos);
      HiveObjectSerializer hos = new NaiiveSerializer();
      WritableHiveObject who = new NoTagWritableHiveObject(null, hos);
      for(int i=0; i < 5; i++) {
        who.setHo(r[i]);
        who.write(dos);
      }

      //System.out.println(new String(bos.getData(), 0, bos.getCount(), "UTF-8"));

      ByteStream.Input bin = new ByteStream.Input(bos.getData(), 0, bos.getCount());
      DataInputStream din = new DataInputStream(bin);
      for(int i=0; i < 5; i++) {
        who.readFields(din);
        HiveObject ho = who.getHo();
        for(int j=0; j<3; j++) {
          SerDeField sdf = ho.getFieldFromExpression(""+j);
          String str = (String)ho.get(sdf).getJavaObject();
          assertEquals(str, new String(""+(i-1+j)));
        }
      }
      System.out.println("testNoTagWritable OK");
    } catch (Exception e) {
      e.printStackTrace();
      throw (e);
    }
  }

  public void testWritableComparable() throws Exception {
    try {
      ByteStream.Output bos = new ByteStream.Output ();
      DataOutputStream dos = new DataOutputStream (bos);
      HiveObjectSerializer hos = new NaiiveSerializer();
      NoTagWritableComparableHiveObject [] who = new NoTagWritableComparableHiveObject [5];
      // 3, 1, 4, 2, 0
      for(int i=0; i < 5; i++) {
        who[i] = new NoTagWritableComparableHiveObject(null, hos);
        who[i].setHo(r[((i+1)*3) % 5]);
        who[i].write(dos);
      }

      ByteStream.Input bin = new ByteStream.Input(bos.getData(), 0, bos.getCount());
      DataInputStream din = new DataInputStream(bin);
      
      for(int i=0; i < 5; i++) {
        who[i].readFields(din);
      }
       
      assertEquals(who[0].compareTo(who[1]) > 0, true);
      assertEquals(who[1].compareTo(who[2]) > 0, false);
      assertEquals(who[2].compareTo(who[3]) > 0, true);
      assertEquals(who[3].compareTo(who[4]) > 0, true);
      System.out.println("testWritableComparable OK");
    } catch (Exception e) {
      e.printStackTrace();
      throw (e);
    }
  }
}
