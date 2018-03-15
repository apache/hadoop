/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.messages;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * Writes outbound HTTP response strings.  We use this rather than the built-in
 * writer so that we can determine content length from the string length instead
 * of possibly falling back to a chunked response.
 */
public final class StringMessageBodyWriter implements
    MessageBodyWriter<String> {
  private static final int CHUNK_SIZE = 8192;

  @Override
  public long getSize(String str, Class<?> type, Type genericType,
                      Annotation[] annotations, MediaType mediaType) {
    return str.length();
  }

  @Override
  public boolean isWriteable(Class<?> type, Type genericType,
                             Annotation[] annotations, MediaType mediaType) {
    return String.class.isAssignableFrom(type);
  }

  @Override
  public void writeTo(String str, Class<?> type, Type genericType,
                      Annotation[] annotations, MediaType mediaType,
                      MultivaluedMap<String, Object> httpHeaders,
                      OutputStream out) throws IOException {
    IOUtils.copyBytes(new ByteArrayInputStream(
        str.getBytes(OzoneUtils.ENCODING)), out, CHUNK_SIZE);
  }
}
