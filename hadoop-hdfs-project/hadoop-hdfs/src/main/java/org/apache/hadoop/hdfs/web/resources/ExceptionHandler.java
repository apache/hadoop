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

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.web.JsonUtil;

/** Handle exceptions. */
@Provider
public class ExceptionHandler implements ExceptionMapper<Exception> {
  public static final Log LOG = LogFactory.getLog(ExceptionHandler.class);

  @Override
  public Response toResponse(final Exception e) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GOT EXCEPITION", e);
    }

    final Response.Status s;
    if (e instanceof SecurityException) {
      s = Response.Status.UNAUTHORIZED;
    } else if (e instanceof FileNotFoundException) {
      s = Response.Status.NOT_FOUND;
    } else if (e instanceof IOException) {
      s = Response.Status.FORBIDDEN;
    } else if (e instanceof UnsupportedOperationException) {
      s = Response.Status.BAD_REQUEST;
    } else {
      s = Response.Status.INTERNAL_SERVER_ERROR;
    }
 
    final String js = JsonUtil.toJsonString(e);
    return Response.status(s).type(MediaType.APPLICATION_JSON).entity(js).build();
  }
}
