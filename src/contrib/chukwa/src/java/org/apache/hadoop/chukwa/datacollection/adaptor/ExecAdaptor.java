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

package org.apache.hadoop.chukwa.datacollection.adaptor;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.inputtools.plugin.ExecPlugin;
import org.apache.log4j.helpers.ISO8601DateFormat;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.*;

public class ExecAdaptor extends ExecPlugin implements Adaptor {

  static final boolean FAKE_LOG4J_HEADER = true;
   
  class RunToolTask extends TimerTask {
    public void run() {
      JSONObject o = execute();
      try {
        
        if(o.getInt("status") == statusKO)
          hardStop();
        
         //FIXME: downstream customers would like timestamps here.
         //Doing that efficiently probably means cutting out all the
         //excess buffer copies here, and appending into an OutputBuffer. 
        byte[] data;
        if(FAKE_LOG4J_HEADER) {
          StringBuilder result = new StringBuilder();
          ISO8601DateFormat dateFormat = new org.apache.log4j.helpers.ISO8601DateFormat();
          result.append(dateFormat.format(new java.util.Date()));
          result.append(" INFO org.apache.hadoop.chukwa.");
          result.append(type);
          result.append(": ");  
          result.append(o.getString("stdout"));
          data = result.toString().getBytes();
        } else {
          String stdout = o.getString("stdout");
          data = stdout.getBytes();
        }
 
        ArrayList<Integer> carriageReturns = new  ArrayList<Integer>();
        for(int i = 0; i < data.length ; ++i)
          if(data[i] == '\n')
            carriageReturns.add(i);
        
        sendOffset += data.length;
        ChunkImpl c = new ChunkImpl(ExecAdaptor.this.type,
            "results from " + cmd, sendOffset , data, ExecAdaptor.this);
        c.setRecordOffsets(carriageReturns);
        dest.add(c);
      } catch(JSONException e ) {
        //FIXME: log this somewhere
      } catch (InterruptedException e)  {
        // TODO Auto-generated catch block
      }catch(AdaptorException e ) {
        //FIXME: log this somewhere
      }
    }
  };
  
  String cmd;
  String type;
  ChunkReceiver dest;
  final java.util.Timer timer;
  long period = 5 * 1000;
  volatile long sendOffset = 0;
  
  public ExecAdaptor() {
    timer = new java.util.Timer();
  }
  
  @Override
  public String getCurrentStatus() throws AdaptorException {
    return cmd;
  }

  @Override
  public void hardStop() throws AdaptorException {
    super.stop();
    timer.cancel();
  }

  @Override
  public long shutdown() throws AdaptorException   { 
    try {
      timer.cancel();
      super.waitFor(); //wait for last data to get pushed out
    } catch(InterruptedException e) {
     return sendOffset; 
    }
    return sendOffset;
  }

  @Override
  public void start(String type, String status, long offset, ChunkReceiver dest)
      throws AdaptorException
  {
    cmd = status;
    this.type = type;
    this.dest = dest;
    this.sendOffset = offset;
    
    TimerTask exec = new RunToolTask();
    timer.schedule(exec, 0L, period);
  }

  @Override
  public String getCmde() {
    return cmd;
  }
  

  @Override
  public String getType() {
    return type;
  }

}
