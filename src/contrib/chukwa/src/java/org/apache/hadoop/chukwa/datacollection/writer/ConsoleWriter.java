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

package org.apache.hadoop.chukwa.datacollection.writer;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.conf.Configuration;

public class ConsoleWriter implements ChukwaWriter {

  boolean printData;
  volatile long dataSize=0;
  final Timer statTimer;
  
  
  private class StatReportingTask extends TimerTask {
    private long lastTs=System.currentTimeMillis();
    private long lastDataSize=0;
    public void run() {
      long time =  System.currentTimeMillis();
      long interval= time - lastTs;
      lastTs = time;
      
      long ds = dataSize;
      long dataRate =  1000 * (ds - lastDataSize) / interval;  //bytes/sec
        //refers only to data field, not including http or chukwa headers
      lastDataSize = ds;
      
      System.out.println("stat=datacollection.writer.ConsoleWriter|dataRate=" + dataRate );
    }
  };
  

  public ConsoleWriter() {
    this(true);
  }
  
  public ConsoleWriter(boolean printData) {
    this.printData = printData;
    statTimer = new Timer();
  }
  
  public void close()
  {
    statTimer.cancel();
  }

  public void init(Configuration conf) throws WriterException
  {
     System.out.println("----  DUMMY HDFS WRITER IN USE ---");

     statTimer.schedule(new StatReportingTask(), 1000,10*1000);
  }

  public void add(Chunk data) throws WriterException
  {
    int startOffset = 0;

    dataSize += data.getData().length;
    if(printData) {
      System.out.println(data.getData().length + " bytes of data in chunk");

      for(int offset: data.getRecordOffsets()) {
        System.out.print(data.getStreamName());
        System.out.print(" ");
        System.out.print(data.getSource());
        System.out.print(" ");
        System.out.print(data.getDataType());
        System.out.print(") ");
        System.out.print(new String(data.getData(), startOffset, offset - startOffset + 1));
        startOffset= offset + 1;
      }
    }
  }

@Override
public void add(List<Chunk> chunks) throws WriterException
{
	for(Chunk chunk: chunks)
	{
		add(chunk);
	}
	
}

}
