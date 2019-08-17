/*
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
package org.apache.hadoop.ozone.s3.exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlRootElement;


/**
 * This class represents exceptions raised from Ozone S3 service.
 *
 * Ref:https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
 */
@XmlRootElement(name = "Error")
@XmlAccessorType(XmlAccessType.NONE)
public class OS3Exception extends  Exception {
  private static final Logger LOG =
      LoggerFactory.getLogger(OS3Exception.class);
  private static ObjectMapper mapper;

  static {
    mapper = new XmlMapper();
    mapper.registerModule(new JaxbAnnotationModule());
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
  }
  @XmlElement(name = "Code")
  private String code;

  @XmlElement(name = "Message")
  private String errorMessage;

  @XmlElement(name = "Resource")
  private String resource;

  @XmlElement(name = "RequestId")
  private String requestId;

  @XmlTransient
  private int httpCode;

  public OS3Exception() {
    //Added for JaxB.
  }

  /**
   * Create an object OS3Exception.
   * @param codeVal
   * @param messageVal
   * @param requestIdVal
   * @param resourceVal
   */
  public OS3Exception(String codeVal, String messageVal, String requestIdVal,
                      String resourceVal) {
    this.code = codeVal;
    this.errorMessage = messageVal;
    this.requestId = requestIdVal;
    this.resource = resourceVal;
  }

  /**
   * Create an object OS3Exception.
   * @param codeVal
   * @param messageVal
   * @param httpCode
   */
  public OS3Exception(String codeVal, String messageVal, int httpCode) {
    this.code = codeVal;
    this.errorMessage = messageVal;
    this.httpCode = httpCode;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public String getRequestId() {
    return requestId;
  }

  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  public String getResource() {
    return resource;
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  public int getHttpCode() {
    return httpCode;
  }

  public void setHttpCode(int httpCode) {
    this.httpCode = httpCode;
  }

  public String toXml() {
    try {
      String val = mapper.writeValueAsString(this);
      LOG.debug("toXml val is {}", val);
      String xmlLine = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + val;
      return xmlLine;
    } catch (Exception ex) {
      LOG.error("Exception occurred {}", ex);
    }

    //When we get exception log it, and return exception as xml from actual
    // exception data. So, falling back to construct from exception.
    String formatString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<Error>" +
        "<Code>%s</Code>" +
        "<Message>%s</Message>" +
        "<Resource>%s</Resource>" +
        "<RequestId>%s</RequestId>" +
        "</Error>";
    return String.format(formatString, this.getCode(),
        this.getErrorMessage(), this.getResource(),
        this.getRequestId());
  }
}
