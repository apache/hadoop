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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.StringInterner;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EventReader implements Closeable {
  private String version;
  private Schema schema;
  private DataInputStream in;
  private Decoder decoder;
  private DatumReader reader;

  /**
   * Create a new Event Reader
   * @param fs
   * @param name
   * @throws IOException
   */
  public EventReader(FileSystem fs, Path name) throws IOException {
    this (fs.open(name));
  }

  /**
   * Create a new Event Reader
   * @param in
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public EventReader(DataInputStream in) throws IOException {
    this.in = in;
    this.version = in.readLine();

    Schema myschema = new SpecificData(Event.class.getClassLoader()).getSchema(Event.class);
    Schema.Parser parser = new Schema.Parser();
    String eventschema = in.readLine();
    if (null != eventschema) {
      try {
        this.schema = parser.parse(eventschema);
        this.reader = new SpecificDatumReader(schema, myschema);
        if (EventWriter.VERSION.equals(version)) {
          this.decoder = DecoderFactory.get().jsonDecoder(schema, in);
        } else if (EventWriter.VERSION_BINARY.equals(version)) {
          this.decoder = DecoderFactory.get().binaryDecoder(in, null);
        } else {
          throw new IOException("Incompatible event log version: " + version);
        }
      } catch (AvroRuntimeException e) {
        throw new IOException(e);
      }
    } else {
      throw new IOException("Event schema string not parsed since its null");
    }
  }
  
  /**
   * Get the next event from the stream
   * @return the next event
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public HistoryEvent getNextEvent() throws IOException {
    Event wrapper;
    try {
      wrapper = (Event)reader.read(null, decoder);
    } catch (EOFException e) {            // at EOF
      return null;
    }
    HistoryEvent result;
    switch (wrapper.getType()) {
    case JOB_SUBMITTED:
      result = new JobSubmittedEvent(); break;
    case JOB_INITED:
      result = new JobInitedEvent(); break;
    case JOB_FINISHED:
      result = new JobFinishedEvent(); break;
    case JOB_PRIORITY_CHANGED:
      result = new JobPriorityChangeEvent(); break;
    case JOB_QUEUE_CHANGED:
      result = new JobQueueChangeEvent(); break;
    case JOB_STATUS_CHANGED:
      result = new JobStatusChangedEvent(); break;
    case JOB_FAILED:
      result = new JobUnsuccessfulCompletionEvent(); break;
    case JOB_KILLED:
      result = new JobUnsuccessfulCompletionEvent(); break;
    case JOB_ERROR:
      result = new JobUnsuccessfulCompletionEvent(); break;
    case JOB_INFO_CHANGED:
      result = new JobInfoChangeEvent(); break;
    case TASK_STARTED:
      result = new TaskStartedEvent(); break;
    case TASK_FINISHED:
      result = new TaskFinishedEvent(); break;
    case TASK_FAILED:
      result = new TaskFailedEvent(); break;
    case TASK_UPDATED:
      result = new TaskUpdatedEvent(); break;
    case MAP_ATTEMPT_STARTED:
      result = new TaskAttemptStartedEvent(); break;
    case MAP_ATTEMPT_FINISHED:
      result = new MapAttemptFinishedEvent(); break;
    case MAP_ATTEMPT_FAILED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case MAP_ATTEMPT_KILLED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case REDUCE_ATTEMPT_STARTED:
      result = new TaskAttemptStartedEvent(); break;
    case REDUCE_ATTEMPT_FINISHED:
      result = new ReduceAttemptFinishedEvent(); break;
    case REDUCE_ATTEMPT_FAILED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case REDUCE_ATTEMPT_KILLED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case SETUP_ATTEMPT_STARTED:
      result = new TaskAttemptStartedEvent(); break;
    case SETUP_ATTEMPT_FINISHED:
      result = new TaskAttemptFinishedEvent(); break;
    case SETUP_ATTEMPT_FAILED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case SETUP_ATTEMPT_KILLED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case CLEANUP_ATTEMPT_STARTED:
      result = new TaskAttemptStartedEvent(); break;
    case CLEANUP_ATTEMPT_FINISHED:
      result = new TaskAttemptFinishedEvent(); break;
    case CLEANUP_ATTEMPT_FAILED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case CLEANUP_ATTEMPT_KILLED:
      result = new TaskAttemptUnsuccessfulCompletionEvent(); break;
    case AM_STARTED:
      result = new AMStartedEvent(); break;
    default:
      throw new RuntimeException("unexpected event type: " + wrapper.getType());
    }
    result.setDatum(wrapper.getEvent());
    return result;
  }

  /**
   * Close the Event reader
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
    in = null;
  }

  static Counters fromAvro(JhCounters counters) {
    Counters result = new Counters();
    if(counters != null) {
      for (JhCounterGroup g : counters.getGroups()) {
        CounterGroup group =
            result.addGroup(StringInterner.weakIntern(g.getName().toString()),
                StringInterner.weakIntern(g.getDisplayName().toString()));
        for (JhCounter c : g.getCounts()) {
          group.addCounter(StringInterner.weakIntern(c.getName().toString()),
              StringInterner.weakIntern(c.getDisplayName().toString()),
                  c.getValue());
        }
      }
    }
    return result;
  }

}
