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
package org.apache.hadoop.ozone.recon.api.types;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * A converter to convert Instant to standard date string.
 */
public class IsoDateAdapter extends XmlAdapter<String, Instant> {

  private DateTimeFormatter iso8861Formatter;

  public IsoDateAdapter() {
    iso8861Formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
            .withZone(ZoneOffset.UTC);
  }

  @Override
  public Instant unmarshal(String v) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public String marshal(Instant v) throws Exception {
    return iso8861Formatter.format(v);
  }
}
