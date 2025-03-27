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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.writer;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.glassfish.jersey.jettison.JettisonJaxbContext;
import org.glassfish.jersey.jettison.JettisonMarshaller;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

/**
 * We have defined a dedicated Writer for ApplicationSubmissionContextInfo,
 * aimed at adapting to the Jersey2 framework to ensure
 * that ApplicationSubmissionContextInfo can be converted into JSON format.
 */
@Provider
@Consumes(MediaType.APPLICATION_JSON)
public class ApplicationSubmissionContextInfoWriter
    implements MessageBodyWriter<ApplicationSubmissionContextInfo> {

  private JettisonMarshaller jettisonMarshaller;
  private Marshaller marshaller;

  public ApplicationSubmissionContextInfoWriter() {
    try {
      JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(
          ApplicationSubmissionContextInfo.class);
      jettisonMarshaller = jettisonJaxbContext.createJsonMarshaller();
      marshaller = jettisonJaxbContext.createMarshaller();
    } catch (JAXBException e) {
    }
  }

  @Override
  public boolean isWriteable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return type == ApplicationSubmissionContextInfo.class;
  }

  @Override
  public void writeTo(ApplicationSubmissionContextInfo applicationSubmissionContextInfo,
      Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
      throws IOException, WebApplicationException {
    StringWriter stringWriter = new StringWriter();
    try {
      if (mediaType.toString().equals(MediaType.APPLICATION_JSON)) {
        jettisonMarshaller.marshallToJSON(applicationSubmissionContextInfo, stringWriter);
        entityStream.write(stringWriter.toString().getBytes(StandardCharsets.UTF_8));
      }

      if (mediaType.toString().equals(MediaType.APPLICATION_XML)) {
        marshaller.marshal(applicationSubmissionContextInfo, stringWriter);
        entityStream.write(stringWriter.toString().getBytes(StandardCharsets.UTF_8));
      }

    } catch (JAXBException e) {
      throw new IOException(e);
    }
  }
}
