/*
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

package org.apache.hadoop.chukwa.util;

import java.util.Random;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.*;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;

public class ConstRateAdaptor  extends Thread implements Adaptor {


  private static final int SLEEP_VARIANCE = 200; 
  private static final int MIN_SLEEP = 300; 
  
  private String type;
  private long offset;
  private int bytesPerSec;
  private ChunkReceiver dest;
  private long adaptorID;
  
  private volatile boolean stopping = false;
  public String getCurrentStatus() throws AdaptorException {
    return type.trim()  + " " + bytesPerSec + " " + offset;
  }

  public void start(long adaptor, String type, String bytesPerSecParam, long offset, ChunkReceiver dest) throws AdaptorException
  {
    try{
      bytesPerSec = Integer.parseInt(bytesPerSecParam.trim());
    } catch(NumberFormatException e) {
      throw new AdaptorException("bad argument to const rate adaptor: [" + bytesPerSecParam + "]");
    }
    this.adaptorID = adaptor;
    this.offset = offset;
    this.type = type;
    this.dest = dest;
    this.setName("ConstRate Adaptor_" + type);
    super.start();  //this is a Thread.start
  }
  
  public String getStreamName() {
    return this.type;
  }
  
  public void run()
  {
    Random r = new Random();
    try{
      while(!stopping) {
        int MSToSleep = r.nextInt(SLEEP_VARIANCE) + MIN_SLEEP; //between 1 and 3 secs
          //FIXME: I think there's still a risk of integer overflow here
        int arraySize = (int) (MSToSleep * (long) bytesPerSec / 1000L);
        byte[] data = new byte[ arraySize];
        r.nextBytes(data);
        offset += data.length;
        ChunkImpl evt = new ChunkImpl(type,"random data source",  offset, data , this);

        dest.add(evt);
        
        Thread.sleep(MSToSleep);
      } //end while
    }  catch(InterruptedException ie)
    {} //abort silently
  }
  
  public String toString() {
    return "const rate " + type;
  }

  public void hardStop() throws AdaptorException {
    stopping = true;
  }

  public long shutdown() throws AdaptorException {
    stopping = true;
    return offset;
  }

  @Override
  public String getType() {
    return type;
  }

}
