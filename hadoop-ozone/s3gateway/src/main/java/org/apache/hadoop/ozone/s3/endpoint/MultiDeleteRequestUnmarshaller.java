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

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.UnmarshallerHandler;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * Custom unmarshaller to read MultiDeleteRequest w/wo namespace.
 */
@Provider
@Produces(MediaType.APPLICATION_XML)
public class MultiDeleteRequestUnmarshaller
    implements MessageBodyReader<MultiDeleteRequest> {

  private final JAXBContext context;
  private final XMLReader xmlReader;

  public MultiDeleteRequestUnmarshaller() {
    try {
      context = JAXBContext.newInstance(MultiDeleteRequest.class);
      SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
      xmlReader = saxParserFactory.newSAXParser().getXMLReader();
    } catch (Exception ex) {
      throw new AssertionError("Can't instantiate MultiDeleteRequest parser",
          ex);
    }
  }

  @Override
  public boolean isReadable(Class<?> type, Type genericType,
      Annotation[] annotations, MediaType mediaType) {
    return type.equals(MultiDeleteRequest.class);
  }

  @Override
  public MultiDeleteRequest readFrom(Class<MultiDeleteRequest> type,
      Type genericType, Annotation[] annotations, MediaType mediaType,
      MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
      throws IOException, WebApplicationException {
    try {
      UnmarshallerHandler unmarshallerHandler =
          context.createUnmarshaller().getUnmarshallerHandler();

      XmlNamespaceFilter filter =
          new XmlNamespaceFilter("http://s3.amazonaws.com/doc/2006-03-01/");
      filter.setContentHandler(unmarshallerHandler);
      filter.setParent(xmlReader);
      filter.parse(new InputSource(entityStream));
      return (MultiDeleteRequest) unmarshallerHandler.getResult();
    } catch (Exception e) {
      throw new WebApplicationException("Can't parse request body to XML.", e);
    }
  }
}
