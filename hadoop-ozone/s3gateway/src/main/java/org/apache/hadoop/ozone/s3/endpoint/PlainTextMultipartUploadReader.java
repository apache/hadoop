/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.endpoint;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * Body reader to accept plain text MPU.
 * <p>
 * Aws s3 api sends a multipartupload request with the content type
 * 'text/plain' in case of using 'aws s3 cp' (instead of aws s3api).
 * <p>
 * Our generic ObjectEndpoint.multipartUpload has a
 * CompleteMultipartUploadRequest parameter, which is required only for the
 * completion request.
 * <p>
 * But JaxRS tries to parse it from the body for the requests and in case of
 * text/plain requests this parsing is failed. This simple BodyReader enables
 * to parse an empty text/plain message and return with an empty completion
 * request.
 */
@Provider
@Consumes("text/plain")
public class PlainTextMultipartUploadReader
    implements MessageBodyReader<CompleteMultipartUploadRequest> {

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return type.equals(CompleteMultipartUploadRequest.class)
        && mediaType.equals(MediaType.TEXT_PLAIN_TYPE);
  }

  @Override
  public CompleteMultipartUploadRequest readFrom(
      Class<CompleteMultipartUploadRequest> type, Type genericType,
      Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
      throws IOException, WebApplicationException {
    return new CompleteMultipartUploadRequest();
  }
}
