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
package org.apache.hadoop.yarn.webapp;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.UnmarshalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.authorize.AuthorizationException;

/**
 * Handle webservices jersey exceptions and create json or xml response
 * with the ExceptionData.
 */
@LimitedPrivate({"YARN", "MapReduce"})
@Singleton
@Provider
public class GenericExceptionHandler implements ExceptionMapper<Exception> {

  public static final Logger LOG = LoggerFactory.getLogger(GenericExceptionHandler.class);

  @Context
  private HttpServletResponse response;

  @Override
  public Response toResponse(Exception e) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("GOT EXCEPTION", e);
    }
    // Don't catch this as filter forward on 404
    // (ServletContainer.FEATURE_FILTER_FORWARD_ON_404)
    // won't work and the web UI won't work!
    if (e instanceof javax.ws.rs.NotFoundException) {
      return ((javax.ws.rs.NotFoundException) e).getResponse();
    }
    // clear content type
    response.setContentType(null);

    // Convert exception
    if (e instanceof RemoteException) {
      e = ((RemoteException) e).unwrapRemoteException();
    }

    // Map response status
    final Response.Status s;
    if (e instanceof SecurityException) {
      s = Response.Status.UNAUTHORIZED;
    } else if (e instanceof AuthorizationException) {
      s = Response.Status.UNAUTHORIZED;
    } else if (e instanceof FileNotFoundException) {
      s = Response.Status.NOT_FOUND;
    } else if (e instanceof NotFoundException) {
      s = Response.Status.NOT_FOUND;
    } else if (e instanceof IOException) {
      s = Response.Status.NOT_FOUND;
    } else if (e instanceof ForbiddenException) {
      s = Response.Status.FORBIDDEN;
    } else if (e instanceof UnsupportedOperationException) {
      s = Response.Status.BAD_REQUEST;
    } else if (e instanceof IllegalArgumentException) {
      s = Response.Status.BAD_REQUEST;
    } else if (e instanceof BadRequestException) {
      s = Response.Status.BAD_REQUEST;
    } else if (e instanceof WebApplicationException
        && e.getCause() instanceof UnmarshalException) {
      s = Response.Status.BAD_REQUEST;
    } else {
      LOG.warn("SERVICE_UNAVAILABLE", e);
      s = Response.Status.SERVICE_UNAVAILABLE;
    }

    // let jaxb handle marshalling data out in the same format requested
    String errorMessage = e.getMessage();
    Throwable cause = e.getCause();
    if (cause != null) {
      errorMessage = cause.getMessage();
    }
    RemoteExceptionData exception = new RemoteExceptionData(e.getClass().getSimpleName(),
        errorMessage, e.getClass().getName());
    return Response.status(s).entity(exception).build();
  }
}
