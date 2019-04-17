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
package org.apache.hadoop.registry.server.dns;

import org.xbill.DNS.DClass;
import org.xbill.DNS.NXTRecord;
import org.xbill.DNS.Name;
import org.xbill.DNS.RRset;
import org.xbill.DNS.Record;
import org.xbill.DNS.SetResponse;
import org.xbill.DNS.Type;
import org.xbill.DNS.Zone;
import org.xbill.DNS.ZoneTransferException;
import org.xbill.DNS.ZoneTransferIn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A zone implementation geared to support some DNSSEC functionality.
 */
public class SecureableZone extends Zone {
  private List<Record> records;

  /**
   * Creates a Zone by doing the specified zone transfer.
   * @param xfrin The incoming zone transfer to execute.
   * @throws IOException if there is an error.
   * @throws ZoneTransferException if there is an error.
   */
  public SecureableZone(ZoneTransferIn xfrin)
      throws IOException, ZoneTransferException {
    super(xfrin);
  }

  /**
   * Creates a Zone by performing a zone transfer to the specified host.
   * @param zone  zone name.
   * @param dclass the dclass
   * @param remote  the remote host.
   * @throws IOException if there is an error.
   * @throws ZoneTransferException if there is an error.
   */
  public SecureableZone(Name zone, int dclass, String remote)
      throws IOException, ZoneTransferException {
    super(zone, dclass, remote);
  }

  /**
   * Creates a Zone from the records in the specified master file.
   * @param zone The name of the zone.
   * @param file The master file to read from.
   * @throws IOException if there is an error.
   */
  public SecureableZone(Name zone, String file) throws IOException {
    super(zone, file);
  }

  /**
   * Creates a Zone from an array of records.
   * @param zone The name of the zone.
   * @param records The records to add to the zone.
   * @throws IOException if there is an error.
   */
  public SecureableZone(Name zone, Record[] records)
      throws IOException {
    super(zone, records);
  }

  /**
   * Adds a Record to the Zone.
   * @param r The record to be added
   * @see Record
   */
  @Override public void addRecord(Record r) {
    if (records == null) {
      records = new ArrayList<Record>();
    }
    super.addRecord(r);
    records.add(r);
  }

  /**
   * Removes a record from the Zone.
   * @param r The record to be removed
   * @see Record
   */
  @Override public void removeRecord(Record r) {
    if (records == null) {
      records = new ArrayList<Record>();
    }
    super.removeRecord(r);
    records.remove(r);
  }

  /**
   * Return a NXT record appropriate for the query.
   * @param queryRecord the query record.
   * @param zone the zone to search.
   * @return  the NXT record describing the insertion point.
   */
  @SuppressWarnings({"unchecked"})
  public Record getNXTRecord(Record queryRecord, Zone zone) {
    Collections.sort(records);

    int index = Collections.binarySearch(records, queryRecord,
        new Comparator<Record>() {
          @Override public int compare(Record r1, Record r2) {
            return r1.compareTo(r2);
          }
        });
    if (index >= 0) {
      return null;
    }
    index = -index - 1;
    if (index >= records.size()) {
      index = records.size() - 1;
    }
    Record base = records.get(index);
    SetResponse sr = zone.findRecords(base.getName(), Type.ANY);
    BitSet bitMap = new BitSet();
    bitMap.set(Type.NXT);
    RRset[] rRsets = sr.answers();
    for (RRset rRset : rRsets) {
      int typeCode = rRset.getType();
      if (typeCode > 0 && typeCode < 128) {
        bitMap.set(typeCode);
      }
    }
    return new NXTRecord(base.getName(), DClass.IN, zone.getSOA().getMinimum(),
        queryRecord.getName(), bitMap);
  }
}
