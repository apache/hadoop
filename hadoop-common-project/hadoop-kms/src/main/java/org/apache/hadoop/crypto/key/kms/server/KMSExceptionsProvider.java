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
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.classification.InterfaceAudience;

import com.sun.jersey.api.container.ContainerException;

import org.apache.hadoop.crypto.key.kms.KMSRESTConstants;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Jersey provider that converts KMS exceptions into detailed HTTP errors.
 */
@Provider
@InterfaceAudience.Private
public class KMSExceptionsProvider implements ExceptionMapper<Exception> {
  private static Logger LOG =
      LoggerFactory.getLogger(KMSExceptionsProvider.class);

  private static final String ENTER = System.getProperty("line.separator");

  protected Response createResponse(Response.Status status, Throwable ex) {
    Map<String, Object> json = new LinkedHashMap<String, Object>();
    json.put(KMSRESTConstants.ERROR_EXCEPTION_JSON, ex.getClass().getName());
    json.put(KMSRESTConstants.ERROR_MESSAGE_JSON, getOneLineMessage(ex));
    log(status, ex);
    return Response.status(status).type(MediaType.APPLICATION_JSON).
        entity(json).build();
  }

  protected String getOneLineMessage(Throwable exception) {
    String message = exception.getMessage();
    if (message != null) {
      int i = message.indexOf(ENTER);
      if (i > -1) {
        message = message.substring(0, i);
      }
    }
    return message;
  }

  /**
   * Maps different exceptions thrown by KMS to HTTP status codes.
   */
  @Override
  public Response toResponse(Exception exception) {
    Response.Status status;
    boolean doAudit = true;
    Throwable throwable = exception;
    if (exception instanceof ContainerException) {
      throwable = exception.getCause();
    }
    if (throwable instanceof SecurityException) {
      status = Response.Status.FORBIDDEN;
    } else if (throwable instanceof AuthenticationException) {
      status = Response.Status.FORBIDDEN;
      // we don't audit here because we did it already when checking access
      doAudit = false;
    } else if (throwable instanceof AuthorizationException) {
      status = Response.Status.UNAUTHORIZED;
      // we don't audit here because we did it already when checking access
      doAudit = false;
    } else if (throwable instanceof AccessControlException) {
      status = Response.Status.FORBIDDEN;
    } else if (exception instanceof IOException) {
      status = Response.Status.INTERNAL_SERVER_ERROR;
    } else if (exception instanceof UnsupportedOperationException) {
      status = Response.Status.BAD_REQUEST;
    } else if (exception instanceof IllegalArgumentException) {
      status = Response.Status.BAD_REQUEST;
    } else {
      status = Response.Status.INTERNAL_SERVER_ERROR;
    }
    if (doAudit) {
      KMSWebApp.getKMSAudit().error(KMSMDCFilter.getUgi(),
          KMSMDCFilter.getMethod(),
          KMSMDCFilter.getURL(), getOneLineMessage(exception));
    }
    return createResponse(status, throwable);
  }

  protected void log(Response.Status status, Throwable ex) {
    UserGroupInformation ugi = KMSMDCFilter.getUgi();
    String method = KMSMDCFilter.getMethod();
    String url = KMSMDCFilter.getURL();
    String msg = getOneLineMessage(ex);
    LOG.warn("User:'{}' Method:{} URL:{} Response:{}-{}", ugi, method, url,
        status, msg, ex);
  }

}
