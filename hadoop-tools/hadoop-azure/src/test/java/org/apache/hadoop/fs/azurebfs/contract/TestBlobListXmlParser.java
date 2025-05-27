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

package org.apache.hadoop.fs.azurebfs.contract;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.assertj.core.api.Assertions;
import java.util.List;

import org.junit.Test;
import org.xml.sax.SAXException;

import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.BlobListXmlParser;

public class TestBlobListXmlParser {
  @Test
  public void testXMLParser() throws Exception {
    String xmlResponseWithDelimiter = ""
        + "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        + "<EnumerationResults ServiceEndpoint=\"https://anujtestfns.blob.core.windows.net/\" ContainerName=\"manualtest\">"
        + "<Delimiter>/</Delimiter>"
        + "<Blobs>"
        + "<BlobPrefix>"
        + "<Name>bye/</Name>"
        + "</BlobPrefix>"
        + "<Blob>"
        + "<Name>explicitDir</Name>"
        + "<Properties>"
        + "<Creation-Time>Tue, 06 Jun 2023 08:34:28 GMT</Creation-Time>"
        + "<Last-Modified>Tue, 06 Jun 2023 08:35:00 GMT</Last-Modified>"
        + "<Etag>0x8DB6668EAB50E67</Etag>"
        + "<Content-Length>0</Content-Length>"
        + "<Content-Type>application/octet-stream</Content-Type>"
        + "<Content-Encoding />"
        + "<Content-Language />"
        + "<Content-CRC64 />"
        + "<Content-MD5>1B2M2Y8AsgTpgAmY7PhCfg==</Content-MD5>"
        + "<Cache-Control />"
        + "<Content-Disposition />"
        + "<BlobType>BlockBlob</BlobType>"
        + "<AccessTier>Hot</AccessTier>"
        + "<AccessTierInferred>true</AccessTierInferred>"
        + "<LeaseStatus>unlocked</LeaseStatus>"
        + "<LeaseState>available</LeaseState>"
        + "<ServerEncrypted>true</ServerEncrypted>"
        + "</Properties>"
        + "<Metadata>"
        + "<hdi_isfolder>true</hdi_isfolder>"
        + "</Metadata>"
        + "<OrMetadata />"
        + "</Blob>"
        + "<BlobPrefix>"
        + "<Name>hello/</Name>"
        + "</BlobPrefix>"
        + "<Blob>"
        + "<Name>splits.txt</Name>"
        + "<Properties>"
        + "<Creation-Time>Tue, 06 Jun 2023 08:33:51 GMT</Creation-Time>"
        + "<Last-Modified>Mon, 19 Jun 2023 04:11:51 GMT</Last-Modified>"
        + "<Etag>0x8DB707B4F77B831</Etag>"
        + "<Content-Length>67681</Content-Length>"
        + "<Content-Type>text/plain</Content-Type>"
        + "<Content-Encoding />"
        + "<Content-Language />"
        + "<Content-CRC64 />"
        + "<Content-MD5>If+Z0+KfGsXjvdpT1Z66NQ==</Content-MD5>"
        + "<Cache-Control />"
        + "<Content-Disposition />"
        + "<BlobType>BlockBlob</BlobType>"
        + "<AccessTier>Hot</AccessTier>"
        + "<AccessTierInferred>true</AccessTierInferred>"
        + "<LeaseStatus>unlocked</LeaseStatus>"
        + "<LeaseState>available</LeaseState>"
        + "<ServerEncrypted>true</ServerEncrypted>"
        + "</Properties>"
        + "<Metadata>"
        + "<hello>hi</hello>"
        + "<unicodeAttribute>%D0%91%d0%BB%D1%8E%D0%B7</unicodeAttribute>"
        + "</Metadata>"
        + "<OrMetadata />"
        + "</Blob>"
        + "</Blobs>"
        + "<NextMarker>TEST_CONTINUATION_TOKEN</NextMarker>"
        + "</EnumerationResults>";
    BlobListResultSchema listResultSchema = getResultSchema(xmlResponseWithDelimiter);
    List<BlobListResultEntrySchema> paths = listResultSchema.paths();
    Assertions.assertThat(paths.size()).isEqualTo(4);
    Assertions.assertThat(paths.get(0).isDirectory()).isEqualTo(true);
    Assertions.assertThat(paths.get(1).isDirectory()).isEqualTo(true);
    Assertions.assertThat(paths.get(2).isDirectory()).isEqualTo(true);
    Assertions.assertThat(paths.get(3).isDirectory()).isEqualTo(false);
    Assertions.assertThat(listResultSchema.getNextMarker()).isNotNull();
  }

  @Test
  public void testEmptyBlobListNullCT() throws Exception {
    String xmlResponse = ""
        + "<?xml version=\"1.0\" encoding=\"utf-8\"?><"
        + "EnumerationResults ServiceEndpoint=\"https://anujtestfns.blob.core.windows.net/\" ContainerName=\"manualtest\">"
        + "<Prefix>abc/</Prefix>"
        + "<Delimiter>/</Delimiter>"
        + "<Blobs /><NextMarker />"
        + "</EnumerationResults>";
    BlobListResultSchema listResultSchema = getResultSchema(xmlResponse);
    List<BlobListResultEntrySchema> paths = listResultSchema.paths();
    Assertions.assertThat(paths.size()).isEqualTo(0);
    Assertions.assertThat(listResultSchema.getNextMarker()).isNull();
  }

  @Test
  public void testEmptyBlobListValidCT() throws Exception {
    String xmlResponse = ""
        + "<?xml version=\"1.0\" encoding=\"utf-8\"?><"
        + "EnumerationResults ServiceEndpoint=\"https://anujtestfns.blob.core.windows.net/\" ContainerName=\"manualtest\">"
        + "<Prefix>abc/</Prefix>"
        + "<Delimiter>/</Delimiter>"
        + "<Blobs />"
        + "<NextMarker>TEST_CONTINUATION_TOKEN</NextMarker>"
        + "</EnumerationResults>";
    BlobListResultSchema listResultSchema = getResultSchema(xmlResponse);
    List<BlobListResultEntrySchema> paths = listResultSchema.paths();
    Assertions.assertThat(paths.size()).isEqualTo(0);
    Assertions.assertThat(listResultSchema.getNextMarker()).isNotNull();
  }

  @Test
  public void testNonEmptyBlobListNullCT() throws Exception {
    String xmlResponse = ""
        + "<?xml version=\"1.0\" encoding=\"utf-8\"?><"
        + "EnumerationResults ServiceEndpoint=\"https://anujtestfns.blob.core.windows.net/\" ContainerName=\"manualtest\">"
        + "<Prefix>abc/</Prefix>"
        + "<Delimiter>/</Delimiter>"
        + "<Blobs>"
        + "<BlobPrefix>"
        + "<Name>bye/</Name>"
        + "</BlobPrefix>"
        + "</Blobs>"
        + "<NextMarker />"
        + "</EnumerationResults>";
    BlobListResultSchema listResultSchema = getResultSchema(xmlResponse);
    List<BlobListResultEntrySchema> paths = listResultSchema.paths();
    Assertions.assertThat(paths.size()).isEqualTo(1);
    Assertions.assertThat(listResultSchema.getNextMarker()).isNull();
  }

  private static final ThreadLocal<SAXParser> SAX_PARSER_THREAD_LOCAL
      = new ThreadLocal<SAXParser>() {
    @Override
    public SAXParser initialValue() {
      SAXParserFactory factory = SAXParserFactory.newInstance();
      factory.setNamespaceAware(true);
      try {
        return factory.newSAXParser();
      } catch (SAXException e) {
        throw new RuntimeException("Unable to create SAXParser", e);
      } catch (ParserConfigurationException e) {
        throw new RuntimeException("Check parser configuration", e);
      }
    }
  };

  private BlobListResultSchema getResultSchema(String xmlResponse) throws Exception {
    byte[] bytes = xmlResponse.getBytes();
    final InputStream stream = new ByteArrayInputStream(bytes);
    final SAXParser saxParser = SAX_PARSER_THREAD_LOCAL.get();
    saxParser.reset();
    BlobListResultSchema listResultSchema = new BlobListResultSchema();
    saxParser.parse(stream, new BlobListXmlParser(listResultSchema, "https://sample.url"));
    return listResultSchema;
  }
}
