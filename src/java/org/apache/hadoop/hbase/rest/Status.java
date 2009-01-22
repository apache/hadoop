/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.rest;

import java.util.HashMap;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.rest.descriptors.ScannerIdentifier;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.serializer.IRestSerializer;
import org.apache.hadoop.hbase.rest.serializer.ISerializable;
import org.apache.hadoop.hbase.util.Bytes;

import agilejson.TOJSON;

public class Status {

  @SuppressWarnings("unused")
  private Log LOG = LogFactory.getLog(Status.class);

  public static final HashMap<Integer, String> statNames = new HashMap<Integer, String>();

  static {
    statNames.put(HttpServletResponse.SC_CONTINUE, "continue");
    statNames.put(HttpServletResponse.SC_SWITCHING_PROTOCOLS,
        "switching protocols");
    statNames.put(HttpServletResponse.SC_OK, "ok");
    statNames.put(HttpServletResponse.SC_CREATED, "created");
    statNames.put(HttpServletResponse.SC_ACCEPTED, "accepted");
    statNames.put(HttpServletResponse.SC_NON_AUTHORITATIVE_INFORMATION,
        "non-authoritative information");
    statNames.put(HttpServletResponse.SC_NO_CONTENT, "no content");
    statNames.put(HttpServletResponse.SC_RESET_CONTENT, "reset content");
    statNames.put(HttpServletResponse.SC_PARTIAL_CONTENT, "partial content");
    statNames.put(HttpServletResponse.SC_MULTIPLE_CHOICES, "multiple choices");
    statNames
        .put(HttpServletResponse.SC_MOVED_PERMANENTLY, "moved permanently");
    statNames
        .put(HttpServletResponse.SC_MOVED_TEMPORARILY, "moved temporarily");
    statNames.put(HttpServletResponse.SC_FOUND, "found");
    statNames.put(HttpServletResponse.SC_SEE_OTHER, "see other");
    statNames.put(HttpServletResponse.SC_NOT_MODIFIED, "not modified");
    statNames.put(HttpServletResponse.SC_USE_PROXY, "use proxy");
    statNames.put(HttpServletResponse.SC_TEMPORARY_REDIRECT,
        "temporary redirect");
    statNames.put(HttpServletResponse.SC_BAD_REQUEST, "bad request");
    statNames.put(HttpServletResponse.SC_UNAUTHORIZED, "unauthorized");
    statNames.put(HttpServletResponse.SC_FORBIDDEN, "forbidden");
    statNames.put(HttpServletResponse.SC_NOT_FOUND, "not found");
    statNames.put(HttpServletResponse.SC_METHOD_NOT_ALLOWED,
        "method not allowed");
    statNames.put(HttpServletResponse.SC_NOT_ACCEPTABLE, "not acceptable");
    statNames.put(HttpServletResponse.SC_PROXY_AUTHENTICATION_REQUIRED,
        "proxy authentication required");
    statNames.put(HttpServletResponse.SC_REQUEST_TIMEOUT, "request timeout");
    statNames.put(HttpServletResponse.SC_CONFLICT, "conflict");
    statNames.put(HttpServletResponse.SC_GONE, "gone");
    statNames.put(HttpServletResponse.SC_LENGTH_REQUIRED, "length required");
    statNames.put(HttpServletResponse.SC_PRECONDITION_FAILED,
        "precondition failed");
    statNames.put(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE,
        "request entity too large");
    statNames.put(HttpServletResponse.SC_REQUEST_URI_TOO_LONG,
        "request uri too long");
    statNames.put(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE,
        "unsupported media type");
    statNames.put(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE,
        "requested range not satisfiable");
    statNames.put(HttpServletResponse.SC_EXPECTATION_FAILED,
        "expectation failed");
    statNames.put(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        "internal server error");
    statNames.put(HttpServletResponse.SC_NOT_IMPLEMENTED, "not implemented");
    statNames.put(HttpServletResponse.SC_BAD_GATEWAY, "bad gateway");
    statNames.put(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
        "service unavailable");
    statNames.put(HttpServletResponse.SC_GATEWAY_TIMEOUT, "gateway timeout");
    statNames.put(HttpServletResponse.SC_HTTP_VERSION_NOT_SUPPORTED,
        "http version not supported");
  }
  protected int statusCode;
  protected HttpServletResponse response;
  protected Object message;
  protected IRestSerializer serializer;
  protected byte[][] pathSegments; 

  public int getStatusCode() {
    return statusCode;
  }

  @TOJSON
  public Object getMessage() {
    return message;
  }

  public static class StatusMessage implements ISerializable {
    int statusCode;
    boolean error;
    Object reason;

    public StatusMessage(int statusCode, boolean error, Object o) {
      this.statusCode = statusCode;
      this.error = error;
      reason = o;
    }

    @TOJSON
    public int getStatusCode() {
      return statusCode;
    }

    @TOJSON
    public boolean getError() {
      return error;
    }

    @TOJSON
    public Object getMessage() {
      return reason;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.hbase.rest.xml.IOutputXML#toXML(org.apache.hadoop.hbase
     * .rest.serializer.IRestSerializer)
     */
    public void restSerialize(IRestSerializer serializer)
        throws HBaseRestException {
      serializer.serializeStatusMessage(this);
    }
  }

  public Status(HttpServletResponse r, IRestSerializer serializer, byte[][] bs) {
    this.setOK();
    this.response = r;
    this.serializer = serializer;
    this.pathSegments = bs;
  }

  // Good Messages
  public void setOK() {
    this.statusCode = HttpServletResponse.SC_OK;
    this.message = new StatusMessage(HttpServletResponse.SC_OK, false, "success");
  }

  public void setOK(Object message) {
    this.statusCode = HttpServletResponse.SC_OK;
    this.message = message;
  }
  
  public void setAccepted() {
    this.statusCode = HttpServletResponse.SC_ACCEPTED;
    this.message = new StatusMessage(HttpServletResponse.SC_ACCEPTED, false, "success");
  }

  public void setExists(boolean error) {
    this.statusCode = HttpServletResponse.SC_CONFLICT;
    this.message = new StatusMessage(statusCode, error, "table already exists");
  }

  public void setCreated() {
    this.statusCode = HttpServletResponse.SC_CREATED;
    this.setOK();
  }  

  public void setScannerCreated(ScannerIdentifier scannerIdentifier) {
    this.statusCode = HttpServletResponse.SC_OK;
    this.message = scannerIdentifier;
    response.addHeader("Location", "/" + Bytes.toString(pathSegments[0])
        + "/scanner/" + scannerIdentifier.getId());
  }
  // Bad Messages

  public void setInternalError(Exception e) {
    this.statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
    this.message = new StatusMessage(statusCode, true, e);
  }

  public void setNoQueryResults() {
    this.statusCode = HttpServletResponse.SC_NOT_FOUND;
    this.message = new StatusMessage(statusCode, true, "no query results");
  }

  public void setConflict(Object message) {
    this.statusCode = HttpServletResponse.SC_CONFLICT;
    this.message = new StatusMessage(statusCode, true, message);
  }

  public void setNotFound(Object message) {
    this.statusCode = HttpServletResponse.SC_NOT_FOUND;
    this.message = new StatusMessage(statusCode, true, message);
  }

  public void setBadRequest(Object message) {
    this.statusCode = HttpServletResponse.SC_BAD_REQUEST;
    this.message = new StatusMessage(statusCode, true, message);
  }

  public void setNotFound() {
    setNotFound("Unable to find requested URI");
  }

  public void setMethodNotImplemented() {
    this.statusCode = HttpServletResponse.SC_METHOD_NOT_ALLOWED;
    this.message = new StatusMessage(statusCode, true, "method not implemented");
  }

  public void setInvalidURI() {
    setInvalidURI("Invalid URI");
  }

  public void setInvalidURI(Object message) {
    this.statusCode = HttpServletResponse.SC_BAD_REQUEST;
    this.message = new StatusMessage(statusCode, true, message);
  }
  
  public void setUnsupportedMediaType(Object message) {
    this.statusCode = HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE;
    this.message = new StatusMessage(statusCode, true, message);
  }
  
  public void setGone() {
    this.statusCode = HttpServletResponse.SC_GONE;
    this.message = new StatusMessage(statusCode, true, "item no longer available");
  }
  

  // Utility
  public void respond() throws HBaseRestException {
    response.setStatus(this.statusCode);
    this.serializer.writeOutput(this.message);
  }

}
