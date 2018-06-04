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
package org.apache.hadoop.hdfs.ipc;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class BufferCallBeforeInitHandler extends ChannelDuplexHandler {

  private enum BufferCallAction {
    FLUSH, FAIL
  }

  public static final class BufferCallEvent {

    public final BufferCallAction action;

    public final IOException error;

    private BufferCallEvent(BufferCallBeforeInitHandler.BufferCallAction action,
        IOException error) {
      this.action = action;
      this.error = error;
    }

    public static BufferCallBeforeInitHandler.BufferCallEvent success() {
      return SUCCESS_EVENT;
    }

    public static BufferCallBeforeInitHandler.BufferCallEvent fail(
        IOException error) {
      return new BufferCallEvent(BufferCallAction.FAIL, error);
    }
  }

  private static final BufferCallEvent SUCCESS_EVENT =
      new BufferCallEvent(BufferCallAction.FLUSH, null);

  private final Map<Integer, Call> id2Call = new HashMap<>();

  @Override
  public void write(ChannelHandlerContext ctx, Object msg,
      ChannelPromise promise) {
    if (msg instanceof Call) {
      Call call = (Call) msg;
      id2Call.put(call.getId(), call);
      // The call is already in track so here we set the write operation as
      // success.
      // We will fail the call directly if we can not write it out.
      promise.trySuccess();
    } else {
      ctx.write(msg, promise);
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
      throws Exception {
    if (evt instanceof BufferCallEvent) {
      BufferCallEvent bcEvt = (BufferCallBeforeInitHandler.BufferCallEvent) evt;
      switch (bcEvt.action) {
      case FLUSH:
        for (Call call : id2Call.values()) {
          ctx.write(call);
        }
        break;
      case FAIL:
        for (Call call : id2Call.values()) {
          call.setException(bcEvt.error);
        }
        break;
      }
      ctx.flush();
      ctx.pipeline().remove(this);
    } else {
      ctx.fireUserEventTriggered(evt);
    }
  }
}