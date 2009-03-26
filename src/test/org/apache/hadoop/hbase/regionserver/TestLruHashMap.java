/**
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HStoreKey;

public class TestLruHashMap extends TestCase {
  private static LruHashMap<HStoreKey, HStoreKey> lru = null;

  private static HStoreKey[] keys = null;
  private static HStoreKey key = null;
  private static HStoreKey tmpKey = null;
  
  private static HStoreKey[] vals = null;
  private static HStoreKey val = null;
  private static HStoreKey tmpVal = null;
  
  private static HStoreKey[] tmpData = null;
  
  //Have to set type
  private static Set<LruHashMap.Entry<HStoreKey, HStoreKey>> hashSet = null;
  private static List<LruHashMap.Entry<HStoreKey, HStoreKey>> entryList = null;
  private static LruHashMap.Entry<HStoreKey, HStoreKey> entry = null;
  
  private static Random rand = null;
  private static int ENTRY_ARRAY_LEN = 2000;
  private static int LOOPS = 10;

  protected void setUp()
  throws Exception{
    super.setUp();
    long maxMemUsage = 10000000L;
    //Using the default values for everything, except memUsage
    lru =  new LruHashMap<HStoreKey, HStoreKey>(maxMemUsage);

    
    rand = new Random();

    keys    = new HStoreKey[ENTRY_ARRAY_LEN];
    vals    = new HStoreKey[ENTRY_ARRAY_LEN];
    tmpData = new HStoreKey[ENTRY_ARRAY_LEN];
  }

  protected void tearDown()
  throws Exception{
    super.tearDown();
  }
  

  /**
   * This test adds data to the Lru and checks that the head and tail pointers
   * are updated correctly
   */
   public void testAdd_Pointers(){
     for(int i=0; i<LOOPS; i++){
       sequential(keys);
       tmpKey = keys[0];
       
       for(HStoreKey key: keys){
         lru.put(key, key);
         assertTrue("headPtr key not correct",
             lru.getHeadPtr().getKey().equals(tmpKey));
         
         assertTrue("tailPtr key not correct",
             lru.getTailPtr().getKey().equals(key));
       }
       lru.clear();
     }
     System.out.println("testAdd_Pointers: OK");
   }  
    
  /**
   * This test adds data to the Lru and checks that the memFree variable never
   * goes below 0  
   */
  public void testAdd_MemUsage_random(){
    for(int i=0; i<LOOPS; i++){
      random(keys);
      
      for(HStoreKey key : keys){
        lru.put(key, key);
  
        assertTrue("Memory usage exceeded!", lru.getMemFree() > 0);
      }

      lru.clear();
    }
    System.out.println("testAdd_MemUsage: OK");
  }
  
  /**
   * This test adds data to the Lru and checks that the memFree variable never
   * goes below 0  
   */
   public void testAdd_MemUsage_sequential(){
     for(int i=0; i<LOOPS; i++){
       sequential(keys);
       
       for(HStoreKey key : keys){
         lru.put(key, key);
   
         assertTrue("Memory usage exceeded!", lru.getMemFree() > 0);
       }

       lru.clear();
     }
     System.out.println("testAdd_MemUsage: OK");
   }
   
  /**
   * This test adds data to the Lru and checks that the order in the lru is the
   * same as the insert order
   */
  public void testAdd_Order()
  throws Exception{
    for(int i=0; i<LOOPS; i++){
      //Adding to Lru
      put();
            
      //Getting order from lru
      entryList = lru.entryLruList();
      
      //Comparing orders
      assertTrue("Different lengths" , keys.length == entryList.size());
      int j = 0;
      for(Map.Entry entry : entryList){
        //Comparing keys
        assertTrue("Different order", keys[j++].equals(entry.getKey()));
      }

      //Clearing the Lru
      lru.clear();  
    }
    System.out.println("testAdd_Order: OK");
  }    
    
    
  /**
   * This test adds data to the Lru, clears it and checks that the memoryUsage
   * looks ok afterwards
   */
  public void testAdd_Clear()
  throws Exception{
    long initMemUsage = 0L;
    long putMemUsage = 0L;
    long clearMemUsage = 0L;
    for(int i=0; i<LOOPS; i++){
      initMemUsage = lru.getMemFree();
    
      //Adding to Lru
      put();
      putMemUsage = lru.getMemFree();

      //Clearing the Lru
      lru.clear();
      clearMemUsage = lru.getMemFree();
      assertTrue("memUsage went down", clearMemUsage <= initMemUsage);
    }
    
    System.out.println("testAdd_Clear: OK");
  }    
    
    
  /**
   * This test adds data to the Lru and checks that all the data that is in
   * the hashSet is also in the EntryList
   */
  public void testAdd_Containment(){
    for(int i=0; i<LOOPS; i++){
      //Adding to Lru
      put();
      
      //Getting HashSet
      hashSet = lru.entryTableSet();
      
      //Getting EntryList
      entryList = lru.entryLruList();
      
      //Comparing
      assertTrue("Wrong size", hashSet.size() == entryList.size());
      for(int j=0; j<entryList.size(); j++){
        assertTrue("Set doesn't contain value from list",
          hashSet.contains(entryList.get(j)));
      }
      
      //Clearing the Lru
      lru.clear();
    }
    System.out.println("testAdd_Containment: OK");
  }
    
    
  /**
   * This test gets an entry from the map and checks that the position of it has
   * been updated afterwards.
   */  
  public void testGet(){
    int getter = 0;
   
    for(int i=0; i<LOOPS; i++){
      //Adding to Lru
      put();
    
      //Getting a random entry from the map
      getter = rand.nextInt(ENTRY_ARRAY_LEN);
      key = keys[getter];
      val = lru.get(key);
   
      //Checking if the entries position has changed
      entryList = lru.entryLruList();
      tmpKey = entryList.get(entryList.size()-1).getKey();
      assertTrue("Get did not put entry first", tmpKey.equals(key));
      
      if(getter != ENTRY_ARRAY_LEN -1){
        tmpKey = entryList.get(getter).getKey();
        assertFalse("Get did leave entry in same position", tmpKey.equals(key));
      }
      
      lru.clear();
    }
    System.out.println("testGet: OK");
  }    
    
  /**
   * Updates an entry in the map and checks that the position of it has been
   * updated afterwards.
   */  
  public void testUpdate(){
    for(int i=0; i<LOOPS; i++){
      //Adding to Lru
      put();
    
      //Getting a random entry from the map
      key = keys[rand.nextInt(ENTRY_ARRAY_LEN)];
      val = random(val);
      
      tmpVal = lru.put(key, val);
    
      //Checking if the value has been updated and that the position i first
      entryList = lru.entryLruList();
      tmpKey = entryList.get(entryList.size()-1).getKey();
      assertTrue("put(update) did not put entry first", tmpKey.equals(key));
      if(!val.equals(tmpVal)){
        assertTrue("Value was not updated",
          entryList.get(entryList.size()-1).getValue().equals(val));
        assertFalse("Value was not updated",
          entryList.get(entryList.size()-1).getValue().equals(tmpVal));
      }
      
      lru.clear();      
    }
    
    System.out.println("testUpdate: OK");
  }    
    
  /**
   * Removes an entry in the map and checks that it is no longer in the
   * entryList nor the HashSet afterwards
   */  
  public void testRemove(){
    for(int i=0; i<LOOPS; i++){
      //Adding to Lru
      put();
      entryList = lru.entryLruList();
      
      //Getting a random entry from the map
      key = keys[rand.nextInt(ENTRY_ARRAY_LEN)];
      val = lru.remove(key);
   
      //Checking key is in list
      entryList = lru.entryLruList();
      for(int j=0; j<entryList.size(); j++){
        assertFalse("Entry found in list after remove",
          entryList.get(j).equals(key));
      }
      lru.clear();
    }
    System.out.println("testRemove: OK");
  }    

  
  //Helpers
  private static void put(){
    //Setting up keys and values
    random(keys);
    vals = keys;
    
    //Inserting into Lru
    for(int i=0; i<keys.length; i++){
      lru.put(keys[i], vals[i]);
    }
  }
  
  // Generating data
  private static HStoreKey random(HStoreKey data){
    return new HStoreKey(Bytes.toBytes(rand.nextInt(ENTRY_ARRAY_LEN)));
  }
  private static void random(HStoreKey[] keys){
    final int LENGTH = keys.length;
    Set<Integer> set = new HashSet<Integer>();
    for(int i=0; i<LENGTH; i++){
      Integer pos = 0;
      while(set.contains(pos = new Integer(rand.nextInt(LENGTH)))){}
      set.add(pos);
      keys[i] = new HStoreKey(Bytes.toBytes(pos));
    }
  }
  
  private static void sequential(HStoreKey[] keys){
    for(int i=0; i<keys.length; i++){
      keys[i] = new HStoreKey(Bytes.toBytes(i));
    }
  }
  

  //testAdd
  private HStoreKey[] mapEntriesToArray(List<LruHashMap.Entry<HStoreKey,
  HStoreKey>> entryList){
    List<HStoreKey> res = new ArrayList<HStoreKey>();
    for(Map.Entry<HStoreKey, HStoreKey> entry : entryList){
      res.add(entry.getKey());
    }  
    return res.toArray(new HStoreKey[0]);
  }
  
}

