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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.util.Calendar;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class ChunkSaver
{
  static Logger log = Logger.getLogger(ChunkSaver.class);
  public static ChukwaRecord saveChunk(Chunk chunk, Throwable throwable,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
  {
    try
    {
      reporter.incrCounter("DemuxError", "count", 1);
      reporter.incrCounter("DemuxError", chunk.getDataType() + "Count", 1);
 
      ChukwaRecord record = new ChukwaRecord();
      long ts = System.currentTimeMillis();
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(ts);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      ChukwaRecordKey key = new ChukwaRecordKey();
      key.setKey("" + calendar.getTimeInMillis() + "/" + chunk.getDataType()
          + "/" + chunk.getSource() + "/" + ts);
      key.setReduceType(chunk.getDataType() + "InError");

      record.setTime(ts);

      record.add(Record.tagsField, chunk.getTags());
      record.add(Record.sourceField, chunk.getSource());
      record.add(Record.applicationField, chunk.getApplication());

      DataOutputBuffer ob = new DataOutputBuffer(chunk
          .getSerializedSizeEstimate());
      chunk.write(ob);
      record.add(Record.chunkDataField, new String(ob.getData()));
      record.add(Record.chunkExceptionField, ExceptionUtil
          .getStackTrace(throwable));
      output.collect(key, record);

      return record;
    }
    catch (Throwable e) 
    {
      e.printStackTrace();
      try
      {
        log.warn("Unable to save a chunk: tags: " 
            + chunk.getTags()   + " - source:"
            + chunk.getSource() + " - dataType: "
            + chunk.getDataType() + " - Stream: " 
            + chunk.getStreamName() + " - SeqId: "
            + chunk.getSeqID() + " - Data: " 
            + new String(chunk.getData()) );
      }
      catch (Throwable e1) 
      {
        e.printStackTrace();
      }
    }
    return null;
  }

}
