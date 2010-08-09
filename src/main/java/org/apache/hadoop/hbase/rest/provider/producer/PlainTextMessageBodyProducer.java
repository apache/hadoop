/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.rest.provider.producer;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.apache.hadoop.hbase.rest.Constants;

/**
 * An adapter between Jersey and Object.toString(). Hooks up plain text output
 * to the Jersey content handling framework. 
 * Jersey will first call getSize() to learn the number of bytes that will be
 * sent, then writeTo to perform the actual I/O.
 */
@Provider
@Produces(Constants.MIMETYPE_TEXT)
public class PlainTextMessageBodyProducer 
  implements MessageBodyWriter<Object> {

  private ThreadLocal<byte[]> buffer = new ThreadLocal<byte[]>();

  @Override
  public boolean isWriteable(Class<?> arg0, Type arg1, Annotation[] arg2,
      MediaType arg3) {
    return true;
  }

	@Override
	public long getSize(Object object, Class<?> type, Type genericType,
			Annotation[] annotations, MediaType mediaType) {
    byte[] bytes = object.toString().getBytes(); 
	  buffer.set(bytes);
    return bytes.length;
	}

	@Override
	public void writeTo(Object object, Class<?> type, Type genericType,
			Annotation[] annotations, MediaType mediaType,
			MultivaluedMap<String, Object> httpHeaders, OutputStream outStream)
			throws IOException, WebApplicationException {
    byte[] bytes = buffer.get();
		outStream.write(bytes);
    buffer.remove();
	}	
}
