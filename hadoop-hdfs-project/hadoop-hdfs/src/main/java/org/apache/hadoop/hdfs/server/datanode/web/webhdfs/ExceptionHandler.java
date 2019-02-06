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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import com.google.common.base.Charsets;
import com.sun.jersey.api.ParamException;
import com.sun.jersey.api.container.ContainerException;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.SecretManager;

import java.io.FileNotFoundException;
import java.io.IOException;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.APPLICATION_JSON_UTF8;

class ExceptionHandler {
  private static final Logger LOG = WebHdfsHandler.LOG;

  static DefaultFullHttpResponse exceptionCaught(Throwable cause) {
    Exception e = cause instanceof Exception ? (Exception) cause : new Exception(cause);

    if (LOG.isTraceEnabled()) {
      LOG.trace("GOT EXCEPTION", e);
    }

    //Convert exception
    if (e instanceof ParamException) {
      final ParamException paramexception = (ParamException)e;
      e = new IllegalArgumentException("Invalid value for webhdfs parameter \""
                                         + paramexception.getParameterName() + "\": "
                                         + e.getCause().getMessage(), e);
    } else if (e instanceof ContainerException || e instanceof SecurityException) {
      e = toCause(e);
    } else if (e instanceof RemoteException) {
      e = ((RemoteException)e).unwrapRemoteException();
    }

    //Map response status
    final HttpResponseStatus s;
    if (e instanceof SecurityException) {
      s = FORBIDDEN;
    } else if (e instanceof AuthorizationException) {
      s = FORBIDDEN;
    } else if (e instanceof FileNotFoundException) {
      s = NOT_FOUND;
    } else if (e instanceof IOException) {
      s = FORBIDDEN;
    } else if (e instanceof UnsupportedOperationException) {
      s = BAD_REQUEST;
    } else if (e instanceof IllegalArgumentException) {
      s = BAD_REQUEST;
    } else {
      LOG.warn("INTERNAL_SERVER_ERROR", e);
      s = INTERNAL_SERVER_ERROR;
    }

    final byte[] js = JsonUtil.toJsonString(e).getBytes(Charsets.UTF_8);
    DefaultFullHttpResponse resp =
      new DefaultFullHttpResponse(HTTP_1_1, s, Unpooled.wrappedBuffer(js));

    resp.headers().set(CONTENT_TYPE, APPLICATION_JSON_UTF8);
    resp.headers().set(CONTENT_LENGTH, js.length);
    return resp;
  }

  private static Exception toCause(Exception e) {
    final Throwable t = e.getCause();
    if (e instanceof SecurityException) {
      // For the issue reported in HDFS-6475, if SecurityException's cause
      // is InvalidToken, and the InvalidToken's cause is StandbyException,
      // return StandbyException; Otherwise, leave the exception as is,
      // since they are handled elsewhere. See HDFS-6588.
      if (t != null && t instanceof SecretManager.InvalidToken) {
        final Throwable t1 = t.getCause();
        if (t1 != null && t1 instanceof StandbyException) {
          e = (StandbyException)t1;
        }
      }
    } else {
      if (t != null && t instanceof Exception) {
        e = (Exception)t;
      }
    }
    return e;
  }

}
