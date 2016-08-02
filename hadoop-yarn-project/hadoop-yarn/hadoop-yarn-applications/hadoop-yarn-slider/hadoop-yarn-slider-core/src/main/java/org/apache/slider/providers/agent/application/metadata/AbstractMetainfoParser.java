/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.providers.agent.application.metadata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.commons.digester.Digester;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * This abstract class provide common functionality to parse metainfo.json for
 * either master package or add on packages.
 */
public abstract class AbstractMetainfoParser {
  protected final GsonBuilder gsonBuilder = new GsonBuilder();
  protected final Gson gson;
  private static final Logger log = LoggerFactory
      .getLogger(AbstractMetainfoParser.class);

  public AbstractMetainfoParser() {
    gson = gsonBuilder.create();
  }

  /**
   * Convert to a JSON string
   *
   * @return a JSON string description
   *
   * @throws IOException Problems mapping/writing the object
   */
  public String toJsonString(Metainfo metaInfo) throws IOException {
    return gson.toJson(metaInfo);
  }

  /**
   * Convert from JSON
   *
   * @param json input
   *
   * @return the parsed JSON
   *
   * @throws IOException IO
   */
  public Metainfo fromJsonString(String json)
      throws IOException {
    return gson.fromJson(json, Metainfo.class);
  }

  /**
   * Parse metainfo from an IOStream
   *
   * @param is
   *
   * @return
   *
   * @throws IOException
   */
  public Metainfo fromJsonStream(InputStream is) throws IOException {
    log.debug("loading from xml stream");
    StringWriter writer = new StringWriter();
    IOUtils.copy(is, writer);
    return fromJsonString(writer.toString());
  }

  /**
   * Parse metainfo from an XML formatted IOStream
   *
   * @param metainfoStream
   *
   * @return
   *
   * @throws IOException
   */
  public Metainfo fromXmlStream(InputStream metainfoStream) throws IOException {
    log.debug("loading from xml stream");
    Digester digester = new Digester();
    digester.setValidating(false);

    composeSchema(digester);

    try {
      return (Metainfo) digester.parse(metainfoStream);
    } catch (IOException e) {
      log.debug("IOException in metainfoparser during fromXmlStream: "
          + e.getMessage());
    } catch (SAXException e) {
      log.debug("SAXException in metainfoparser during fromXmlStream: "
          + e.getMessage());
    } finally {
      if (metainfoStream != null) {
        metainfoStream.close();
      }
    }

    return null;
  }

  /**
   * Compose the schema for the metainfo
   *
   * @param Digester - The Digester object we passed in to compose the schema
   *
   * @return
   *
   * @throws IOException
   */
  abstract protected void composeSchema(Digester digester);
}
