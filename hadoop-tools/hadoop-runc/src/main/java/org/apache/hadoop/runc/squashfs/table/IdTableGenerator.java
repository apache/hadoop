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

package org.apache.hadoop.runc.squashfs.table;

import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockRef;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.DECIMAL;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.DumpOptions.UNSIGNED;
import static org.apache.hadoop.runc.squashfs.util.BinUtils.dumpBin;

public class IdTableGenerator {

  private final List<Integer> forward = new ArrayList<>();
  private final SortedMap<Long, Short> reverse = new TreeMap<>();

  public short addUidGid(int value) {
    Long key = Long.valueOf(value & 0xffffffffL);
    Short result = reverse.get(key);
    if (result != null) {
      return result.shortValue();
    }
    forward.add(value);
    result = Short.valueOf((short) (forward.size() - 1));
    reverse.put(key, result);
    return result.shortValue();
  }

  public int getIdCount() {
    return forward.size();
  }

  public List<MetadataBlockRef> save(MetadataWriter writer) throws IOException {

    List<MetadataBlockRef> idRefs = new ArrayList<>();

    int index = 0;
    for (int i = 0; i < forward.size(); i++) {
      if (index % IdTable.ENTRIES_PER_BLOCK == 0) {
        idRefs.add(writer.getCurrentReference());
      }
      int value = forward.get(i).intValue();
      writer.writeInt(value);
      index++;
    }

    return idRefs;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(String.format("id-table-generator: {%n"));
    int width = 17;
    dumpBin(buf, width, "count", forward.size(), DECIMAL, UNSIGNED);
    for (int i = 0; i < forward.size(); i++) {
      dumpBin(buf, width, String.format("mappings[%d]", i), forward.get(i),
          DECIMAL, UNSIGNED);
    }
    buf.append("}");
    return buf.toString();
  }

}
