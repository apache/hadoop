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

import java.io.IOException;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task.CombinerRunner;
import org.apache.hadoop.mapred.nativetask.Command;
import org.apache.hadoop.mapred.nativetask.CommandDispatcher;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.DataChannel;
import org.apache.hadoop.mapred.nativetask.ICombineHandler;
import org.apache.hadoop.mapred.nativetask.INativeHandler;
import org.apache.hadoop.mapred.nativetask.NativeBatchProcessor;
import org.apache.hadoop.mapred.nativetask.TaskContext;
import org.apache.hadoop.mapred.nativetask.serde.SerializationFramework;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CombinerHandler<K, V> implements ICombineHandler, CommandDispatcher {
  public static final String NAME = "NativeTask.CombineHandler";
  private static final Logger LOG =
      LoggerFactory.getLogger(NativeCollectorOnlyHandler.class);
  public static final Command LOAD = new Command(1, "Load");
  public static final Command COMBINE = new Command(4, "Combine");
  public final CombinerRunner<K, V> combinerRunner;

  private final INativeHandler nativeHandler;
  private final BufferPuller puller;
  private final BufferPusher<K, V> kvPusher;
  private boolean closed = false;

  public static <K, V> ICombineHandler create(TaskContext context)
    throws IOException, ClassNotFoundException {
    final JobConf conf = new JobConf(context.getConf());
    conf.set(Constants.SERIALIZATION_FRAMEWORK,
        String.valueOf(SerializationFramework.WRITABLE_SERIALIZATION.getType()));
    String combinerClazz = conf.get(Constants.MAPRED_COMBINER_CLASS);
    if (null == combinerClazz) {
      combinerClazz = conf.get(MRJobConfig.COMBINE_CLASS_ATTR);
    }

    if (null == combinerClazz) {
      return null;
    } else {
      LOG.info("NativeTask Combiner is enabled, class = " + combinerClazz);
    }

    final Counter combineInputCounter = context.getTaskReporter().getCounter(
        TaskCounter.COMBINE_INPUT_RECORDS);

    final CombinerRunner<K, V> combinerRunner = CombinerRunner.create(
        conf, context.getTaskAttemptId(),
        combineInputCounter, context.getTaskReporter(), null);

    final INativeHandler nativeHandler = NativeBatchProcessor.create(
      NAME, conf, DataChannel.INOUT);
    @SuppressWarnings("unchecked")
    final BufferPusher<K, V> pusher = new BufferPusher<K, V>((Class<K>)context.getInputKeyClass(),
        (Class<V>)context.getInputValueClass(),
        nativeHandler);
    final BufferPuller puller = new BufferPuller(nativeHandler);
    return new CombinerHandler<K, V>(nativeHandler, combinerRunner, puller, pusher);
  }

  public CombinerHandler(INativeHandler nativeHandler, CombinerRunner<K, V> combiner,
                         BufferPuller puller, BufferPusher<K, V> kvPusher)
    throws IOException {
    this.nativeHandler = nativeHandler;
    this.combinerRunner = combiner;
    this.puller = puller;
    this.kvPusher = kvPusher;
    nativeHandler.setCommandDispatcher(this);
    nativeHandler.setDataReceiver(puller);
  }

  @Override
  public ReadWriteBuffer onCall(Command command, ReadWriteBuffer parameter) throws IOException {
 if (null == command) {
      return null;
    }
    if (command.equals(COMBINE)) {
      combine();
    }
    return null;

  }

  @Override
  public void combine() throws IOException{
    try {
      puller.reset();
      combinerRunner.combine(puller, kvPusher);
      kvPusher.flush();
      return;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getId() {
    return nativeHandler.getNativeHandler();
  }

  @Override
  public void close() throws IOException {

    if (closed) {
      return;
    }

    if (null != puller) {
      puller.close();
    }

    if (null != kvPusher) {
      kvPusher.close();
    }

    if (null != nativeHandler) {
      nativeHandler.close();
    }
    closed = true;
  }
}
