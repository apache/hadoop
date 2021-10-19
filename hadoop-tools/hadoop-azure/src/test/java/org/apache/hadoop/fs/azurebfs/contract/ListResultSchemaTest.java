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

package org.apache.hadoop.fs.azurebfs.contract;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the JSON parsing for the listfilestatus response to ListResultSchema
 */
public class ListResultSchemaTest {

  /**
   * Test parsing a JSON which matches the properties in the ListResultSchema
   * and ListResultEntrySchema
   * <p>
   * {
   * "paths": [
   * {
   * "contentLength": "0",
   * "etag": "0x8D8186452785ADA",
   * "group": "$superuser",
   * "lastModified": "Wed, 24 Jun 2020 17:30:43 GMT",
   * "name": "dest/filename",
   * "owner": "$superuser",
   * "permissions": "rw-r--r--"
   * }
   * ]
   * }
   */
  @Test
  public void testMatchingJSON() throws IOException {

    String matchingJson =
        "{ \"paths\": [ { \"contentLength\": \"0\", \"etag\": "
            + "\"0x8D8186452785ADA\", \"group\": \"$superuser\", "
            + "\"lastModified\": \"Wed, 24 Jun 2020 17:30:43 GMT\", \"name\": "
            + "\"dest/filename\", \"owner\": \"$superuser\", \"permissions\": "
            + "\"rw-r--r--\" } ] } ";

    final ObjectMapper objectMapper = new ObjectMapper();
    final ListResultSchema listResultSchema = objectMapper
        .readValue(matchingJson, ListResultSchema.class);

    assertThat(listResultSchema.paths().size())
        .describedAs("Only one path is expected as present in the input JSON")
        .isEqualTo(1);

    ListResultEntrySchema path = listResultSchema.paths().get(0);
    assertThat(path.contentLength())
        .describedAs("contentLength should match the value in the input JSON")
        .isEqualTo(0L);
    assertThat(path.eTag())
        .describedAs("eTag should match the value in the input JSON")
        .isEqualTo("0x8D8186452785ADA");
    assertThat(path.group())
        .describedAs("group should match the value in the input JSON")
        .isEqualTo("$superuser");
    assertThat(path.lastModified())
        .describedAs("lastModified should match the value in the input JSON")
        .isEqualTo("Wed, 24 Jun 2020 17:30:43 GMT");
    assertThat(path.name())
        .describedAs("lastModified should match the value in the input JSON")
        .isEqualTo("dest/filename");
    assertThat(path.owner())
        .describedAs("lastModified should match the value in the input JSON")
        .isEqualTo("$superuser");
    assertThat(path.permissions())
        .describedAs("lastModified should match the value in the input JSON")
        .isEqualTo("rw-r--r--");
  }

  /**
   * Test parsing a JSON which matches the properties in the ListResultSchema
   * and ListResultEntrySchema along with an unknown property
   * <p>
   * {
   * "paths": [
   * {
   * "contentLength": "0",
   * "unknownProperty": "132374934429527192",
   * "etag": "0x8D8186452785ADA",
   * "group": "$superuser",
   * "lastModified": "Wed, 24 Jun 2020 17:30:43 GMT",
   * "name": "dest/filename",
   * "owner": "$superuser",
   * "permissions": "rw-r--r--"
   * }
   * ]
   * }
   */
  @Test
  public void testJSONWithUnknownFields() throws IOException {

    String matchingJson = "{ \"paths\": [ { \"contentLength\": \"0\", "
        + "\"unknownProperty\": \"132374934429527192\", \"etag\": "
        + "\"0x8D8186452785ADA\", \"group\": \"$superuser\", "
        + "\"lastModified\": \"Wed, 24 Jun 2020 17:30:43 GMT\", \"name\": "
        + "\"dest/filename\", \"owner\": \"$superuser\", \"permissions\": "
        + "\"rw-r--r--\" } ] } ";

    final ObjectMapper objectMapper = new ObjectMapper();
    final ListResultSchema listResultSchema = objectMapper
        .readValue(matchingJson, ListResultSchema.class);

    assertThat(listResultSchema.paths().size())
        .describedAs("Only one path is expected as present in the input JSON")
        .isEqualTo(1);

    ListResultEntrySchema path = listResultSchema.paths().get(0);
    assertThat(path.contentLength())
        .describedAs("contentLength should match the value in the input JSON")
        .isEqualTo(0L);
    assertThat(path.eTag())
        .describedAs("eTag should match the value in the input JSON")
        .isEqualTo("0x8D8186452785ADA");
    assertThat(path.group())
        .describedAs("group should match the value in the input JSON")
        .isEqualTo("$superuser");
    assertThat(path.lastModified())
        .describedAs("lastModified should match the value in the input JSON")
        .isEqualTo("Wed, 24 Jun 2020 17:30:43 GMT");
    assertThat(path.name())
        .describedAs("lastModified should match the value in the input JSON")
        .isEqualTo("dest/filename");
    assertThat(path.owner())
        .describedAs("lastModified should match the value in the input JSON")
        .isEqualTo("$superuser");
    assertThat(path.permissions())
        .describedAs("lastModified should match the value in the input JSON")
        .isEqualTo("rw-r--r--");
  }

}
