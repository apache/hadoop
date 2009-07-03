/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.stargate.provider.consumer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.stargate.Constants;
import org.apache.hadoop.hbase.stargate.model.IProtobufWrapper;

/**
 * Adapter for hooking up Jersey content processing dispatch to
 * IProtobufWrapper interface capable handlers for decoding protobuf input.
 */
@Provider
@Consumes(Constants.MIMETYPE_PROTOBUF)
public class ProtobufMessageBodyConsumer 
    implements MessageBodyReader<IProtobufWrapper> {
  private static final Log LOG =
    LogFactory.getLog(ProtobufMessageBodyConsumer.class);

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return IProtobufWrapper.class.isAssignableFrom(type);
  }

  @Override
  public IProtobufWrapper readFrom(Class<IProtobufWrapper> type, Type genericType,
      Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders, InputStream inputStream)
      throws IOException, WebApplicationException {
    IProtobufWrapper obj = null;
    try {
      obj = type.newInstance();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int read;
      do {
        read = inputStream.read(buffer, 0, buffer.length);
        if (read > 0) {
          baos.write(buffer, 0, read);
        }
      } while (read > 0);
      if (LOG.isDebugEnabled()) {
        LOG.debug(getClass() + ": read " + baos.size() + " bytes from " +
          inputStream);
      }
      obj = obj.getObjectFromMessage(baos.toByteArray());
    } catch (InstantiationException e) {
      throw new WebApplicationException(e);
    } catch (IllegalAccessException e) {
      throw new WebApplicationException(e);
    }
    return obj;
  }
}
