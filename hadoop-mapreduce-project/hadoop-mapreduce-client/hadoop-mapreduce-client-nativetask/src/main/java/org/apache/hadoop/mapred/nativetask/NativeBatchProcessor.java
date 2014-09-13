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

package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.nativetask.buffer.BufferType;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.apache.hadoop.mapred.nativetask.buffer.OutputBuffer;
import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;
import org.apache.hadoop.mapred.nativetask.util.ConfigUtil;

/**
 * used to create channel, transfer data and command between Java and native
 */
@InterfaceAudience.Private
public class NativeBatchProcessor implements INativeHandler {
  private static Log LOG = LogFactory.getLog(NativeBatchProcessor.class);

  private final String nativeHandlerName;
  private long nativeHandlerAddr;

  private boolean isInputFinished = false;

  // << Field used directly in Native, the name must NOT be changed
  private ByteBuffer rawOutputBuffer;
  private ByteBuffer rawInputBuffer;
  // >>

  private InputBuffer in;
  private OutputBuffer out;

  private CommandDispatcher commandDispatcher;
  private DataReceiver dataReceiver;

  static {
    if (NativeRuntime.isNativeLibraryLoaded()) {
      InitIDs();
    }
  }

  public static INativeHandler create(String nativeHandlerName,
      Configuration conf, DataChannel channel) throws IOException {

    final int bufferSize = conf.getInt(Constants.NATIVE_PROCESSOR_BUFFER_KB,
        1024) * 1024;

    LOG.info("NativeHandler: direct buffer size: " + bufferSize);

    OutputBuffer out = null;
    InputBuffer in = null;

    switch (channel) {
    case IN:
      in = new InputBuffer(BufferType.DIRECT_BUFFER, bufferSize);
      break;
    case OUT:
      out = new OutputBuffer(BufferType.DIRECT_BUFFER, bufferSize);
      break;
    case INOUT:
      in = new InputBuffer(BufferType.DIRECT_BUFFER, bufferSize);
      out = new OutputBuffer(BufferType.DIRECT_BUFFER, bufferSize);
      break;
    case NONE:
    }

    final INativeHandler handler = new NativeBatchProcessor(nativeHandlerName,
        in, out);
    handler.init(conf);
    return handler;
  }

  protected NativeBatchProcessor(String nativeHandlerName, InputBuffer input,
      OutputBuffer output) throws IOException {
    this.nativeHandlerName = nativeHandlerName;

    if (null != input) {
      this.in = input;
      this.rawInputBuffer = input.getByteBuffer();
    }
    if (null != output) {
      this.out = output;
      this.rawOutputBuffer = output.getByteBuffer();
    }
  }

  @Override
  public void setCommandDispatcher(CommandDispatcher handler) {
    this.commandDispatcher = handler;
  }

  @Override
  public void init(Configuration conf) throws IOException {
    this.nativeHandlerAddr = NativeRuntime
        .createNativeObject(nativeHandlerName);
    if (this.nativeHandlerAddr == 0) {
      throw new RuntimeException("Native object create failed, class: "
          + nativeHandlerName);
    }
    setupHandler(nativeHandlerAddr, ConfigUtil.toBytes(conf));
  }

  @Override
  public synchronized void close() throws IOException {
    if (nativeHandlerAddr != 0) {
      NativeRuntime.releaseNativeObject(nativeHandlerAddr);
      nativeHandlerAddr = 0;
    }
    IOUtils.cleanup(LOG, in);
    in = null;
  }

  @Override
  public long getNativeHandler() {
    return nativeHandlerAddr;
  }

  @Override
  public ReadWriteBuffer call(Command command, ReadWriteBuffer parameter)
      throws IOException {
    final byte[] bytes = nativeCommand(nativeHandlerAddr, command.id(),
        null == parameter ? null : parameter.getBuff());

    final ReadWriteBuffer result = new ReadWriteBuffer(bytes);
    result.setWritePoint(bytes.length);
    return result;
  }

  @Override
  public void sendData() throws IOException {
    nativeProcessInput(nativeHandlerAddr, rawOutputBuffer.position());
    rawOutputBuffer.position(0);
  }

  @Override
  public void finishSendData() throws IOException {
    if (null == rawOutputBuffer || isInputFinished) {
      return;
    }

    sendData();
    nativeFinish(nativeHandlerAddr);
    isInputFinished = true;
  }

  private byte[] sendCommandToJava(int command, byte[] data) throws IOException {
    try {

      final Command cmd = new Command(command);
      ReadWriteBuffer param = null;

      if (null != data) {
        param = new ReadWriteBuffer();
        param.reset(data);
        param.setWritePoint(data.length);
      }

      if (null != commandDispatcher) {
        ReadWriteBuffer result = null;

        result = commandDispatcher.onCall(cmd, param);
        if (null != result) {
          return result.getBuff();
        } else {
          return null;
        }
      } else {
        return null;
      }

    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  /**
   * Called by native side, clean output buffer so native side can continue
   * processing
   */
  private void flushOutput(int length) throws IOException {

    if (null != rawInputBuffer) {
      rawInputBuffer.position(0);
      rawInputBuffer.limit(length);

      if (null != dataReceiver) {
        try {
          dataReceiver.receiveData();
        } catch (IOException e) {
          e.printStackTrace();
          throw e;
        }
      }
    }
  }

  /**
   * Cache JNI field & method ids
   */
  private static native void InitIDs();

  /**
   * Setup native side BatchHandler
   */
  private native void setupHandler(long nativeHandlerAddr, byte[][] configs);

  /**
   * Let native side to process data in inputBuffer
   */
  private native void nativeProcessInput(long handler, int length);

  /**
   * Notice native side input is finished
   */
  private native void nativeFinish(long handler);

  /**
   * Send control message to native side
   */
  private native byte[] nativeCommand(long handler, int cmd, byte[] parameter);

  /**
   * Load data from native
   */
  private native void nativeLoadData(long handler);

  protected void finishOutput() {
  }

  @Override
  public InputBuffer getInputBuffer() {
    return this.in;
  }

  @Override
  public OutputBuffer getOutputBuffer() {
    return this.out;
  }

  @Override
  public void loadData() throws IOException {
    nativeLoadData(nativeHandlerAddr);
    //
    // return call(Command.CMD_LOAD, param);
  }

  @Override
  public void setDataReceiver(DataReceiver handler) {
    this.dataReceiver = handler;
  }

  @Override
  public String name() {
    return nativeHandlerName;
  }
}
