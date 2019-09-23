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

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.s3.commontypes.IsoDateAdapter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.time.Instant;

/**
 * Copy object Response.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ListAllMyBucketsResult",
    namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class CopyObjectResponse {

  @XmlJavaTypeAdapter(IsoDateAdapter.class)
  @XmlElement(name = "LastModified")
  private Instant lastModified;

  @XmlElement(name = "ETag")
  private String eTag;


  public Instant getLastModified() {
    return lastModified;
  }

  public void setLastModified(Instant lastModified) {
    this.lastModified = lastModified;
  }

  public String getETag() {
    return eTag;
  }

  public void setETag(String tag) {
    this.eTag = tag;
  }


}
