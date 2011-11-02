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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

/**
 * Event Writer is an utility class used to write events to the underlying
 * stream. Typically, one event writer (which translates to one stream) 
 * is created per job 
 * 
 */
class EventWriter {
  static final String VERSION = "Avro-Json";

  private FSDataOutputStream out;
  private DatumWriter<Event> writer =
    new SpecificDatumWriter<Event>(Event.class);
  private Encoder encoder;
  private static final Log LOG = LogFactory.getLog(EventWriter.class);
  
  EventWriter(FSDataOutputStream out) throws IOException {
    this.out = out;
    out.writeBytes(VERSION);
    out.writeBytes("\n");
    out.writeBytes(Event.SCHEMA$.toString());
    out.writeBytes("\n");
    this.encoder =  EncoderFactory.get().jsonEncoder(Event.SCHEMA$, out);
  }
  
  synchronized void write(HistoryEvent event) throws IOException { 
    Event wrapper = new Event();
    wrapper.type = event.getEventType();
    wrapper.event = event.getDatum();
    writer.write(wrapper, encoder);
    encoder.flush();
    out.writeBytes("\n");
  }
  
  void flush() throws IOException {
    encoder.flush();
    out.flush();
    out.hflush();
  }

  void close() throws IOException {
    try {
      encoder.flush();
      out.close();
      out = null;
    } finally {
      IOUtils.cleanup(LOG, out);
    }
  }

  private static final Schema GROUPS =
    Schema.createArray(JhCounterGroup.SCHEMA$);

  private static final Schema COUNTERS =
    Schema.createArray(JhCounter.SCHEMA$);

  static JhCounters toAvro(Counters counters) {
    return toAvro(counters, "COUNTERS");
  }
  static JhCounters toAvro(Counters counters, String name) {
    JhCounters result = new JhCounters();
    result.name = new Utf8(name);
    result.groups = new ArrayList<JhCounterGroup>(0);
    if (counters == null) return result;
    for (CounterGroup group : counters) {
      JhCounterGroup g = new JhCounterGroup();
      g.name = new Utf8(group.getName());
      g.displayName = new Utf8(group.getDisplayName());
      g.counts = new ArrayList<JhCounter>(group.size());
      for (Counter counter : group) {
        JhCounter c = new JhCounter();
        c.name = new Utf8(counter.getName());
        c.displayName = new Utf8(counter.getDisplayName());
        c.value = counter.getValue();
        g.counts.add(c);
      }
      result.groups.add(g);
    }
    return result;
  }

}
