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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.reader;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.glassfish.jersey.jettison.JettisonJaxbContext;
import org.glassfish.jersey.jettison.JettisonUnmarshaller;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * We have defined a dedicated Reader for ApplicationSubmissionContextInfo,
 * aimed at adapting to the Jersey2 framework
 * to ensure that JSON can be converted into ApplicationSubmissionContextInfo.
 */
@Provider
@Consumes(MediaType.APPLICATION_JSON)
public class ApplicationSubmissionContextInfoReader
    implements MessageBodyReader<ApplicationSubmissionContextInfo> {

  private JettisonUnmarshaller jsonUnmarshaller;

  public ApplicationSubmissionContextInfoReader() {
    try {
      JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(
          ApplicationSubmissionContextInfo.class);
      jsonUnmarshaller = jettisonJaxbContext.createJsonUnmarshaller();
    } catch (JAXBException e) {
    }
  }

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return type == ApplicationSubmissionContextInfo.class;
  }

  @Override
  public ApplicationSubmissionContextInfo readFrom(Class<ApplicationSubmissionContextInfo> type,
      Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
      throws IOException, WebApplicationException {
    try {
      return jsonUnmarshaller.unmarshalFromJSON(entityStream,
          ApplicationSubmissionContextInfo.class);
    } catch (JAXBException e) {
      throw new IOException(e);
    }
  }
}
