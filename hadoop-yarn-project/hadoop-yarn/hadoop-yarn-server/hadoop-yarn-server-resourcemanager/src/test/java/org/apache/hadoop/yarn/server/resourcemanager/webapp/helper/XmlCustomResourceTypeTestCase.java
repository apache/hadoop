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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.helper;

import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.http.JettyUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * This class hides the implementation details of how to verify the structure of
 * XML responses. Tests should only provide the path of the
 * {@link WebResource}, the response from the resource and
 * the verifier Consumer to
 * {@link XmlCustomResourceTypeTestCase#verify(Consumer)}. An instance of
 * {@link JSONObject} will be passed to that consumer to be able to
 * verify the response.
 */
public class XmlCustomResourceTypeTestCase {
  private static final Logger LOG =
      LoggerFactory.getLogger(XmlCustomResourceTypeTestCase.class);

  private WebResource path;
  private BufferedClientResponse response;
  private Document parsedResponse;

  public XmlCustomResourceTypeTestCase(WebResource path,
                                       BufferedClientResponse response) {
    this.path = path;
    verifyStatus(response);
    this.response = response;
  }

  private void verifyStatus(BufferedClientResponse response) {
    String responseStr = response.getEntity(String.class);
    assertEquals("HTTP status should be 200, " +
                    "status info: " + response.getStatusInfo() +
                    " response as string: " + responseStr,
            200, response.getStatus());
  }

  public void verify(Consumer<Document> verifier) {
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getType().toString());

    parsedResponse = parseXml(response);
    logResponse(parsedResponse);
    verifier.accept(parsedResponse);
  }

  private Document parseXml(BufferedClientResponse response) {
    try {
      String xml = response.getEntity(String.class);
      DocumentBuilder db =
          DocumentBuilderFactory.newInstance().newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));

      return db.parse(is);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void logResponse(Document doc) {
    String responseStr = response.getEntity(String.class);
    LOG.info("Raw response from service URL {}: {}", path.toString(),
        responseStr);
    LOG.info("Parsed response from service URL {}: {}", path.toString(),
        toXml(doc));
  }

  public static String toXml(Node node) {
    StringWriter writer;
    try {
      TransformerFactory tf = TransformerFactory.newInstance();
      Transformer transformer = tf.newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.setOutputProperty(
          "{http://xml.apache.org/xslt}indent" + "-amount", "2");
      writer = new StringWriter();
      transformer.transform(new DOMSource(node), new StreamResult(writer));
    } catch (TransformerException e) {
      throw new RuntimeException(e);
    }

    return writer.getBuffer().toString();
  }
}
