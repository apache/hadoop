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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.Response;


/** Http operation parameter. */
public abstract class HttpOpParam<E extends Enum<E> & HttpOpParam.Op>
    extends EnumParam<E> {
  /** Parameter name. */
  public static final String NAME = "op";

  /** Default parameter value. */
  public static final String DEFAULT = NULL;

  /** Http operation types */
  public enum Type {
    GET, PUT, POST, DELETE
  }

  /** Http operation interface. */
  public interface Op {
    /** @return the Http operation type. */
    Type getType();

    /** @return true if the operation cannot use a token */
    boolean getRequireAuth();

    /** @return true if the operation will do output. */
    boolean getDoOutput();

    /** @return true if the operation will be redirected. */
    boolean getRedirect();

    /** @return true the expected http response code. */
    int getExpectedHttpResponseCode();

    /** @return a URI query string. */
    String toQueryString();
  }

  /** Expects HTTP response 307 "Temporary Redirect". */
  public static class TemporaryRedirectOp implements Op {
    static final TemporaryRedirectOp CREATE = new TemporaryRedirectOp(
        PutOpParam.Op.CREATE);
    static final TemporaryRedirectOp APPEND = new TemporaryRedirectOp(
        PostOpParam.Op.APPEND);
    static final TemporaryRedirectOp OPEN = new TemporaryRedirectOp(
        GetOpParam.Op.OPEN);
    static final TemporaryRedirectOp GETFILECHECKSUM = new TemporaryRedirectOp(
        GetOpParam.Op.GETFILECHECKSUM);

    static final List<TemporaryRedirectOp> values
        = Collections.unmodifiableList(Arrays.asList(CREATE, APPEND, OPEN,
                                       GETFILECHECKSUM));

    /** Get an object for the given op. */
    public static TemporaryRedirectOp valueOf(final Op op) {
      for(TemporaryRedirectOp t : values) {
        if (op == t.op) {
          return t;
        }
      }
      throw new IllegalArgumentException(op + " not found.");
    }

    private final Op op;

    private TemporaryRedirectOp(final Op op) {
      this.op = op;
    }

    @Override
    public Type getType() {
      return op.getType();
    }

    @Override
    public boolean getRequireAuth() {
      return op.getRequireAuth();
    }

    @Override
    public boolean getDoOutput() {
      return false;
    }

    @Override
    public boolean getRedirect() {
      return false;
    }

    /** Override the original expected response with "Temporary Redirect". */
    @Override
    public int getExpectedHttpResponseCode() {
      return Response.Status.TEMPORARY_REDIRECT.getStatusCode();
    }

    @Override
    public String toQueryString() {
      return op.toQueryString();
    }
  }

  /** @return the parameter value as a string */
  @Override
  public String getValueString() {
    return value.toString();
  }

  HttpOpParam(final Domain<E> domain, final E value) {
    super(domain, value);
  }
}
