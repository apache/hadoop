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

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.chukwa.util.RecordConstants;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public abstract class AbstractProcessor implements MapProcessor
{
  static Logger log = Logger.getLogger(AbstractProcessor.class);
  
  Calendar calendar = Calendar.getInstance();
  byte[] bytes;
  int[] recordOffsets;
  int currentPos = 0;
  int startOffset = 0;

  ChukwaArchiveKey archiveKey = null;
  ChukwaRecordKey key = new ChukwaRecordKey();
  Chunk chunk = null;

  boolean chunkInErrorSaved = false;
  OutputCollector<ChukwaRecordKey, ChukwaRecord> output = null;
  Reporter reporter = null;

  public AbstractProcessor()
  {
  }

  protected abstract void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable;

  protected void saveChunkInError(Throwable throwable)
  {
    if (chunkInErrorSaved == false)
    {
      try
      {
        ChunkSaver.saveChunk(chunk, throwable, output, reporter);
        chunkInErrorSaved = true;
      } catch (Exception e)
      {
        e.printStackTrace();
      }
    }

  }

  public void process(ChukwaArchiveKey archiveKey, Chunk chunk,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
  {
    chunkInErrorSaved = false;
    
    this.archiveKey = archiveKey;
    this.output = output;
    this.reporter = reporter;
    
    reset(chunk);
    
    while (hasNext())
    {
      try
      {
        parse(nextLine(), output, reporter);
      } catch (Throwable e)
      {
        saveChunkInError(e);
      }
    }
  }

  protected void buildGenericRecord(ChukwaRecord record, String body,
      long timestamp, String dataSource)
  {
    calendar.setTimeInMillis(timestamp);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    key.setKey("" + calendar.getTimeInMillis() + "/" + chunk.getSource() + "/"
        + timestamp);
    key.setReduceType(dataSource);

    if (body != null)
    {
      record.add(Record.bodyField, body);
    }
    record.setTime(timestamp);

    record.add(Record.tagsField, chunk.getTags());
    record.add(Record.sourceField, chunk.getSource());
    record.add(Record.applicationField, chunk.getApplication());

  }

  protected void reset(Chunk chunk)
  {
    this.chunk = chunk;
    this.bytes = chunk.getData();
    this.recordOffsets = chunk.getRecordOffsets();
    currentPos = 0;
    startOffset = 0;
  }

  protected boolean hasNext()
  {
    return (currentPos < recordOffsets.length);
  }

  protected String nextLine()
  {
    String log = new String(bytes, startOffset, (recordOffsets[currentPos]
        - startOffset + 1));
    startOffset = recordOffsets[currentPos] + 1;
    currentPos++;
    return RecordConstants.recoverRecordSeparators("\n", log);
  }
}
