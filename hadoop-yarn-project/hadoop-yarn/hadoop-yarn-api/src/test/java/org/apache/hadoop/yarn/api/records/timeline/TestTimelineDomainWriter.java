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

package org.apache.hadoop.yarn.api.records.timeline;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.core.MediaType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.api.records.timeline.writer.TimelineDomainWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestTimelineDomainWriter {

  @Test
  void testDocumentedFieldNames() throws Exception {
    TimelineDomain domain = new TimelineDomain();
    domain.setId("id");
    domain.setDescription("description");
    domain.setOwner("owner");
    domain.setReaders("readers");
    domain.setWriters("writers");
    domain.setCreatedTime(1L);
    domain.setModifiedTime(2L);

    JsonNode json = new ObjectMapper().readTree(write(domain));
    assertEquals(1L, json.get("createdtime").asLong());
    assertEquals(2L, json.get("modifiedtime").asLong());
    assertFalse(json.has("createdTime"), json.toString());
    assertFalse(json.has("modifiedTime"), json.toString());
    for (String name : new String[] {"id", "description", "owner", "readers", "writers"}) {
      assertTrue(json.has(name), name + " missing from " + json);
    }
  }

  @Test
  void testUnsetTimesAreOmitted() throws Exception {
    TimelineDomain domain = new TimelineDomain();
    domain.setId("id");

    JsonNode json = new ObjectMapper().readTree(write(domain));
    assertFalse(json.has("createdtime"), json.toString());
    assertFalse(json.has("modifiedtime"), json.toString());
  }

  @Test
  void testBothTimeSpellingsAreRead() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TimelineDomain documented = mapper.readValue(
        "{\"id\":\"d\",\"createdtime\":1,\"modifiedtime\":2}", TimelineDomain.class);
    assertEquals(1L, documented.getCreatedTime());
    assertEquals(2L, documented.getModifiedTime());

    TimelineDomain camelCase = mapper.readValue(
        "{\"id\":\"d\",\"createdTime\":1,\"modifiedTime\":2}", TimelineDomain.class);
    assertEquals(1L, camelCase.getCreatedTime());
    assertEquals(2L, camelCase.getModifiedTime());
  }

  private String write(TimelineDomain domain) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new TimelineDomainWriter().writeTo(domain, TimelineDomain.class,
        TimelineDomain.class, null, MediaType.APPLICATION_JSON_TYPE, null, out);
    return out.toString(StandardCharsets.UTF_8.name());
  }
}
