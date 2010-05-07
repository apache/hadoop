/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.GetDeleteTracker.Delete;
import org.apache.hadoop.hbase.util.Bytes;


public class TestGetDeleteTracker extends HBaseTestCase implements HConstants {

  private static final boolean PRINT = true;

  private byte [] col1 = null;
  private byte [] col2 = null;

  private int col1Len = 0;
  private int col2Len = 0;

  private byte [] empty = null;

  private long ts1 = 0L;
  private long ts2 = 0L;
  private long ts3 = 0L;


  private Delete del10 = null;
  private Delete del11 = null;
  private Delete delQf10 = null;
  private Delete delQf11 = null;
  private Delete delFam10 = null;

  private Delete del20 = null;
  private Delete del21 = null;
  private Delete delQf20 = null;
  private Delete delQf21 = null;
  private Delete delFam20 = null;


  private Delete del30 = null;

  GetDeleteTracker dt = null;
  private byte del = KeyValue.Type.Delete.getCode();
  private byte delCol = KeyValue.Type.DeleteColumn.getCode();
  private byte delFam = KeyValue.Type.DeleteFamily.getCode();

  protected void setUp() throws Exception {
    super.setUp();
    dt = new GetDeleteTracker();
    col1 = "col".getBytes();
    col2 = "col2".getBytes();
    col1Len = col1.length;
    col2Len = col2.length;

    empty = new byte[0];

    //ts1
    ts1 = System.nanoTime();
    del10 = new Delete(col1, 0, col1Len, del, ts1);
    del11 = new Delete(col2, 0, col2Len, del, ts1);
    delQf10 = new Delete(col1, 0, col1Len, delCol, ts1);
    delQf11 = new Delete(col2, 0, col2Len, delCol, ts1);
    delFam10 = new Delete(empty, 0, 0, delFam, ts1);

    //ts2
    ts2 = System.nanoTime();
    del20 = new Delete(col1, 0, col1Len, del, ts2);
    del21 = new Delete(col2, 0, col2Len, del, ts2);
    delQf20 = new Delete(col1, 0, col1Len, delCol, ts2);
    delQf21 = new Delete(col2, 0, col2Len, delCol, ts2);
    delFam20 = new Delete(empty, 0, 0, delFam, ts1);

    //ts3
    ts3 = System.nanoTime();
    del30 = new Delete(col1, 0, col1Len, del, ts3);
  }

  public void testUpdate_CompareDeletes() {
    GetDeleteTracker.DeleteCompare res = null;


    //Testing Delete and Delete
    res = dt.compareDeletes(del10, del10);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_OLD_NEXT_BOTH, res);

    //Testing Delete qf1 and Delete qf2 and <==>
    res = dt.compareDeletes(del10, del11);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_OLD_NEXT_OLD, res);
    res = dt.compareDeletes(del11, del10);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_NEW_NEXT_NEW, res);

    //Testing Delete ts1 and Delete ts2 and <==>
    res = dt.compareDeletes(del10, del20);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_NEW_NEXT_NEW, res);
    res = dt.compareDeletes(del20, del10);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_OLD_NEXT_OLD, res);



    //Testing DeleteColumn and DeleteColumn
    res = dt.compareDeletes(delQf10, delQf10);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_OLD_NEXT_BOTH, res);

    //Testing DeleteColumn qf1 and DeleteColumn qf2 and <==>
    res = dt.compareDeletes(delQf10, delQf11);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_OLD_NEXT_OLD, res);
    res = dt.compareDeletes(delQf11, delQf10);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_NEW_NEXT_NEW, res);

    //Testing DeleteColumn ts1 and DeleteColumn ts2 and <==>
    res = dt.compareDeletes(delQf10, delQf20);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_NEW_NEXT_BOTH, res);
    res = dt.compareDeletes(delQf20, delQf10);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_OLD_NEXT_BOTH, res);



    //Testing Delete and DeleteColumn and <==>
    res = dt.compareDeletes(del10, delQf10);
    assertEquals(DeleteTracker.DeleteCompare.NEXT_OLD, res);
    res = dt.compareDeletes(delQf10, del10);
    assertEquals(DeleteTracker.DeleteCompare.NEXT_NEW, res);

    //Testing Delete qf1 and DeleteColumn qf2 and <==>
    res = dt.compareDeletes(del10, delQf11);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_OLD_NEXT_OLD, res);
    res = dt.compareDeletes(delQf11, del10);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_NEW_NEXT_NEW, res);

    //Testing Delete qf2 and DeleteColumn qf1 and <==>
    res = dt.compareDeletes(del11, delQf10);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_NEW_NEXT_NEW, res);
    res = dt.compareDeletes(delQf10, del11);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_OLD_NEXT_OLD, res);

    //Testing Delete ts2 and DeleteColumn ts1 and <==>
    res = dt.compareDeletes(del20, delQf10);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_OLD_NEXT_OLD, res);
    res = dt.compareDeletes(delQf10, del20);
    assertEquals(DeleteTracker.DeleteCompare.INCLUDE_NEW_NEXT_NEW, res);

    //Testing Delete ts1 and DeleteColumn ts2 and <==>
    res = dt.compareDeletes(del10, delQf20);
    assertEquals(DeleteTracker.DeleteCompare.NEXT_OLD, res);
    res = dt.compareDeletes(delQf20, del10);
    assertEquals(DeleteTracker.DeleteCompare.NEXT_NEW, res);

  }

  public void testUpdate(){
    //Building lists
    List<Delete> dels1 = new ArrayList<Delete>();
    dels1.add(delQf10);
    dels1.add(del21);

    List<Delete> dels2 = new ArrayList<Delete>();
    dels2.add(delFam10);
    dels2.add(del30);
    dels2.add(delQf20);

    List<Delete> res = new ArrayList<Delete>();
    res.add(del30);
    res.add(delQf20);
    res.add(del21);

    //Adding entries
    for(Delete del : dels1){
      dt.add(del.buffer, del.qualifierOffset, del.qualifierLength,
          del.timestamp, del.type);
    }

    //update()
    dt.update();

    //Check deleteList
    List<Delete> delList = dt.deletes;
    assertEquals(dels1.size(), delList.size());
    for(int i=0; i<dels1.size(); i++){
      assertEquals(0, Bytes.compareTo(dels1.get(i).buffer,
          delList.get(i).buffer));
      assertEquals(dels1.get(i).qualifierOffset, delList.get(i).qualifierOffset);
      assertEquals(dels1.get(i).qualifierLength, delList.get(i).qualifierLength);
      assertEquals(dels1.get(i).timestamp, delList.get(i).timestamp);
      assertEquals(dels1.get(i).type, delList.get(i).type);
    }

    //Add more entries
    for(Delete del : dels2){
      dt.add(del.buffer, del.qualifierOffset, del.qualifierLength,
          del.timestamp, del.type);
    }
    //Update()
    dt.update();

    //Check deleteList
    delList = dt.deletes;

    for(int i=0; i<res.size(); i++){
      assertEquals(0, Bytes.compareTo(res.get(i).buffer,
          delList.get(i).buffer));
      assertEquals(res.get(i).qualifierOffset, delList.get(i).qualifierOffset);
      assertEquals(res.get(i).qualifierLength, delList.get(i).qualifierLength);
      assertEquals(res.get(i).timestamp, delList.get(i).timestamp);
      assertEquals(res.get(i).type, delList.get(i).type);
      if(PRINT){
        System.out.println("Qf " +new String(delList.get(i).buffer) +
            ", timestamp, " +delList.get(i).timestamp+
            ", type " +KeyValue.Type.codeToType(delList.get(i).type));
      }
    }

  }

  /**
   * Test if a KeyValue is in the lists of deletes already. Cases that needs to
   * be tested are:
   * Not deleted
   * Deleted by a Delete
   * Deleted by a DeleteColumn
   * Deleted by a DeleteFamily
   */
  public void testIsDeleted_NotDeleted(){
    //Building lists
    List<Delete> dels = new ArrayList<Delete>();
    dels.add(delQf10);
    dels.add(del21);

    //Adding entries
    for(Delete del : dels){
      dt.add(del.buffer, del.qualifierOffset, del.qualifierLength,
          del.timestamp, del.type);
    }
    //update()
    dt.update();
    assertEquals(false, dt.isDeleted(col2, 0, col2Len, ts3));
    assertEquals(false, dt.isDeleted(col2, 0, col2Len, ts1));
  }
  public void testIsDeleted_Delete(){
    //Building lists
    List<Delete> dels = new ArrayList<Delete>();
    dels.add(del21);

    //Adding entries
    for(Delete del : dels){
      dt.add(del.buffer, del.qualifierOffset, del.qualifierLength,
          del.timestamp, del.type);
    }

    //update()
    dt.update();

    assertEquals(true, dt.isDeleted(col2, 0, col2Len, ts2));
  }

  public void testIsDeleted_DeleteColumn(){
    //Building lists
    List<Delete> dels = new ArrayList<Delete>();
    dels.add(delQf21);

    //Adding entries
    for(Delete del : dels){
      dt.add(del.buffer, del.qualifierOffset, del.qualifierLength,
          del.timestamp, del.type);
    }

    //update()
    dt.update();

    assertEquals(true, dt.isDeleted(col2, 0, col2Len, ts1));
  }

  public void testIsDeleted_DeleteFamily(){
    //Building lists
    List<Delete> dels = new ArrayList<Delete>();
    dels.add(delFam20);

    //Adding entries
    for(Delete del : dels){
      dt.add(del.buffer, del.qualifierOffset, del.qualifierLength,
          del.timestamp, del.type);
    }

    //update()
    dt.update();

    assertEquals(true, dt.isDeleted(col2, 0, col2Len, ts1));
  }

  // HBASE-1951
  public void testStackOverflow() {
    List<Delete> dels = new ArrayList<Delete>();
    Delete adel = new Delete(col1, 0, col1Len, del, 0L);
    for(long i = 0; i < 9000; i++) {
      dt.add(adel.buffer, adel.qualifierOffset, adel.qualifierLength,
          i, adel.type);
    }


    //update()
    dt.update();
    assertEquals(false, dt.isDeleted(col2, 0, col2Len, 7000000));
  }

}
