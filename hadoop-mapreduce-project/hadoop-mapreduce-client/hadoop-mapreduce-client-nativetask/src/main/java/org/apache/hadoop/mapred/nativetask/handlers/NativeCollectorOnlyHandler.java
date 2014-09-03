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

package org.apache.hadoop.mapred.nativetask.handlers;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.CommandDispatcher;
import org.apache.hadoop.mapred.nativetask.DataChannel;
import org.apache.hadoop.mapred.nativetask.ICombineHandler;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.TaskContext;
import org.apache.hadoop.mapred.nativetask.util.NativeTaskOutput;
import org.apache.hadoop.mapred.nativetask.util.OutputUtil;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;

/**
 * Java Record Reader + Java Mapper + Native Collector
 */
@SuppressWarnings("unchecked")
@InterfaceAudience.Private
public class NativeCollectorOnlyHandler<K, V> implements CommandDispatcher, Closeable {

  public static final String NAME = "NativeTask.MCollectorOutputHandler";
  private static Log LOG = LogFactory.getLog(NativeCollectorOnlyHandler.class);
  public static final Command GET_OUTPUT_PATH =
      new Command(100, "GET_OUTPUT_PATH");
  public static final Command GET_OUTPUT_INDEX_PATH =
      new Command(101, "GET_OUTPUT_INDEX_PATH");
  public static final Command GET_SPILL_PATH =
      new Command(102, "GET_SPILL_PATH");
  public static final Command GET_COMBINE_HANDLER =
      new Command(103, "GET_COMBINE_HANDLER");
  
  private NativeTaskOutput output;
  private int spillNumber = 0;
  private ICombineHandler combinerHandler = null;
  private final BufferPusher<K, V> kvPusher;
  private final INativeHandler nativeHandler;
  private boolean closed = false;

  public static <K, V> NativeCollectorOnlyHandler<K, V> create(TaskContext context)
    throws IOException {

    
    ICombineHandler combinerHandler = null;
    try {
      final TaskContext combineContext = context.copyOf();
      combineContext.setInputKeyClass(context.getOutputKeyClass());
      combineContext.setInputValueClass(context.getOutputValueClass());

      combinerHandler = CombinerHandler.create(combineContext);
    } catch (final ClassNotFoundException e) {
      throw new IOException(e);
    }
    
    if (null != combinerHandler) {
      LOG.info("[NativeCollectorOnlyHandler] combiner is not null");
    }

    final INativeHandler nativeHandler = NativeBatchProcessor.create(
      NAME, context.getConf(), DataChannel.OUT);
    final BufferPusher<K, V> kvPusher = new BufferPusher<K, V>(
        (Class<K>)context.getOutputKeyClass(),
        (Class<V>)context.getOutputValueClass(),
        nativeHandler);

    return new NativeCollectorOnlyHandler<K, V>(context, nativeHandler, kvPusher, combinerHandler);
  }

  protected NativeCollectorOnlyHandler(TaskContext context, INativeHandler nativeHandler,
      BufferPusher<K, V> kvPusher, ICombineHandler combiner) throws IOException {
    Configuration conf = context.getConf();
    TaskAttemptID id = context.getTaskAttemptId();
    if (null == id) {
      this.output = OutputUtil.createNativeTaskOutput(conf, "");
    } else {
      this.output = OutputUtil.createNativeTaskOutput(context.getConf(), context.getTaskAttemptId()
        .toString());
    }
    this.combinerHandler = combiner;
    this.kvPusher = kvPusher;
    this.nativeHandler = nativeHandler;
    nativeHandler.setCommandDispatcher(this);
  }

  public void collect(K key, V value, int partition) throws IOException {
    kvPusher.collect(key, value, partition);
  };

  public void flush() throws IOException {
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    if (null != kvPusher) {
      kvPusher.close();
    }

    if (null != combinerHandler) {
      combinerHandler.close();
    }

    if (null != nativeHandler) {
      nativeHandler.close();
    }
    closed = true;
  }

  @Override
  public ReadWriteBuffer onCall(Command command, ReadWriteBuffer parameter) throws IOException {
    Path p = null;
    if (null == command) {
      return null;
    }
        
    if (command.equals(GET_OUTPUT_PATH)) {
      p = output.getOutputFileForWrite(-1);
    } else if (command.equals(GET_OUTPUT_INDEX_PATH)) {
      p = output.getOutputIndexFileForWrite(-1);
    } else if (command.equals(GET_SPILL_PATH)) {
      p = output.getSpillFileForWrite(spillNumber++, -1);
      
    } else if (command.equals(GET_COMBINE_HANDLER)) {
      if (null == combinerHandler) {
        return null;
      }
      final ReadWriteBuffer result = new ReadWriteBuffer(8);
      
      result.writeLong(combinerHandler.getId());
      return result;
    } else {
      throw new IOException("Illegal command: " + command.toString());
    }
    if (p != null) {
      final ReadWriteBuffer result = new ReadWriteBuffer();
      result.writeString(p.toUri().getPath());
      return result;
    } else {
      throw new IOException("MapOutputFile can't allocate spill/output file");
    }
  }
}
