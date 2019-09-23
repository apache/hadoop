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

package org.apache.hadoop.ozone.s3.endpoint;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.ozone.s3.commontypes.CommonPrefix;
import org.apache.hadoop.ozone.s3.commontypes.KeyMetadata;

/**
 * Response from the ListObject RPC Call.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ListBucketResult", namespace = "http://s3.amazonaws"
    + ".com/doc/2006-03-01/")
public class ListObjectResponse {

  @XmlElement(name = "Name")
  private String name;

  @XmlElement(name = "Prefix")
  private String prefix;

  @XmlElement(name = "Marker")
  private String marker;

  @XmlElement(name = "MaxKeys")
  private int maxKeys;

  @XmlElement(name = "KeyCount")
  private int keyCount;

  @XmlElement(name = "Delimiter")
  private String delimiter = "/";

  @XmlElement(name = "EncodingType")
  private String encodingType = "url";

  @XmlElement(name = "IsTruncated")
  private boolean isTruncated;

  @XmlElement(name = "NextContinuationToken")
  private String nextToken;

  @XmlElement(name = "continueToken")
  private String continueToken;

  @XmlElement(name = "Contents")
  private List<KeyMetadata> contents = new ArrayList<>();

  @XmlElement(name = "CommonPrefixes")
  private List<CommonPrefix> commonPrefixes = new ArrayList<>();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public String getMarker() {
    return marker;
  }

  public void setMarker(String marker) {
    this.marker = marker;
  }

  public int getMaxKeys() {
    return maxKeys;
  }

  public void setMaxKeys(int maxKeys) {
    this.maxKeys = maxKeys;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  public String getEncodingType() {
    return encodingType;
  }

  public void setEncodingType(String encodingType) {
    this.encodingType = encodingType;
  }

  public boolean isTruncated() {
    return isTruncated;
  }

  public void setTruncated(boolean truncated) {
    isTruncated = truncated;
  }

  public List<KeyMetadata> getContents() {
    return contents;
  }

  public void setContents(
      List<KeyMetadata> contents) {
    this.contents = contents;
  }

  public List<CommonPrefix> getCommonPrefixes() {
    return commonPrefixes;
  }

  public void setCommonPrefixes(
      List<CommonPrefix> commonPrefixes) {
    this.commonPrefixes = commonPrefixes;
  }

  public void addKey(KeyMetadata keyMetadata) {
    contents.add(keyMetadata);
  }

  public void addPrefix(String relativeKeyName) {
    commonPrefixes.add(new CommonPrefix(relativeKeyName));
  }

  public String getNextToken() {
    return nextToken;
  }

  public void setNextToken(String nextToken) {
    this.nextToken = nextToken;
  }

  public String getContinueToken() {
    return continueToken;
  }

  public void setContinueToken(String continueToken) {
    this.continueToken = continueToken;
  }

  public int getKeyCount() {
    return keyCount;
  }

  public void setKeyCount(int keyCount) {
    this.keyCount = keyCount;
  }
}
