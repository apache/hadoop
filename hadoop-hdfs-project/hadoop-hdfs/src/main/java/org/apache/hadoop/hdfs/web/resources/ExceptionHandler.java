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
package org.apache.hadoop.hdfs.web.resources;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.ParamException;
import com.sun.jersey.api.container.ContainerException;

/** Handle exceptions. */
@Provider
public class ExceptionHandler implements ExceptionMapper<Exception> {
  public static final Logger LOG =
      LoggerFactory.getLogger(ExceptionHandler.class);

  private static Exception toCause(Exception e) {
    final Throwable t = e.getCause();    
    if (e instanceof SecurityException) {
      // For the issue reported in HDFS-6475, if SecurityException's cause
      // is InvalidToken, and the InvalidToken's cause is StandbyException,
      // return StandbyException; Otherwise, leave the exception as is,
      // since they are handled elsewhere. See HDFS-6588.
      if (t != null && t instanceof InvalidToken) {
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

  private @Context HttpServletResponse response;

  @Override
  public Response toResponse(Exception e) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GOT EXCEPITION", e);
    }

    //clear content type
    response.setContentType(null);

    //Convert exception
    if (e instanceof ParamException) {
      final ParamException paramexception = (ParamException)e;
      e = new IllegalArgumentException("Invalid value for webhdfs parameter \""
          + paramexception.getParameterName() + "\": "
          + e.getCause().getMessage(), e);
    }
    if (e instanceof ContainerException) {
      e = toCause(e);
    }
    if (e instanceof RemoteException) {
      e = ((RemoteException)e).unwrapRemoteException();
    }

    if (e instanceof SecurityException) {
      e = toCause(e);
    }
    
    //Map response status
    final Response.Status s;
    if (e instanceof SecurityException) {
      s = Response.Status.FORBIDDEN;
    } else if (e instanceof AuthorizationException) {
      s = Response.Status.FORBIDDEN;
    } else if (e instanceof FileNotFoundException) {
      s = Response.Status.NOT_FOUND;
    } else if (e instanceof IOException) {
      s = Response.Status.FORBIDDEN;
    } else if (e instanceof UnsupportedOperationException) {
      s = Response.Status.BAD_REQUEST;
    } else if (e instanceof IllegalArgumentException) {
      s = Response.Status.BAD_REQUEST;
    } else {
      LOG.warn("INTERNAL_SERVER_ERROR", e);
      s = Response.Status.INTERNAL_SERVER_ERROR;
    }
 
    final String js = JsonUtil.toJsonString(e);
    return Response.status(s).type(MediaType.APPLICATION_JSON).entity(js).build();
  }
  
  @VisibleForTesting
  public void initResponse(HttpServletResponse response) {
    this.response = response;
  }
}
