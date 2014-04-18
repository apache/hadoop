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
package org.apache.hadoop.hdfs.server.datanode.web.resources;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.io.IOUtils;

/**
 * A response entity for a HdfsDataInputStream.
 */
public class OpenEntity {
  private final HdfsDataInputStream in;
  private final long length;
  private final int outBufferSize;
  private final DFSClient dfsclient;
  
  OpenEntity(final HdfsDataInputStream in, final long length,
      final int outBufferSize, final DFSClient dfsclient) {
    this.in = in;
    this.length = length;
    this.outBufferSize = outBufferSize;
    this.dfsclient = dfsclient;
  }
  
  /**
   * A {@link MessageBodyWriter} for {@link OpenEntity}.
   */
  @Provider
  public static class Writer implements MessageBodyWriter<OpenEntity> {

    @Override
    public boolean isWriteable(Class<?> clazz, Type genericType,
        Annotation[] annotations, MediaType mediaType) {
      return clazz == OpenEntity.class
          && MediaType.APPLICATION_OCTET_STREAM_TYPE.isCompatible(mediaType);
    }

    @Override
    public long getSize(OpenEntity e, Class<?> type, Type genericType,
        Annotation[] annotations, MediaType mediaType) {
      return e.length;
    }

    @Override
    public void writeTo(OpenEntity e, Class<?> type, Type genericType,
        Annotation[] annotations, MediaType mediaType,
        MultivaluedMap<String, Object> httpHeaders, OutputStream out
        ) throws IOException {
      try {
        byte[] buf = new byte[e.outBufferSize];
        long remaining = e.length;
        while (remaining > 0) {
          int read = e.in.read(buf, 0, (int)Math.min(buf.length, remaining));
          if (read == -1) { // EOF
            break;
          }
          out.write(buf, 0, read);
          out.flush();
          remaining -= read;
        }
      } finally {
        IOUtils.cleanup(DatanodeWebHdfsMethods.LOG, e.in);
        IOUtils.cleanup(DatanodeWebHdfsMethods.LOG, e.dfsclient);
      }
    }
  }
}