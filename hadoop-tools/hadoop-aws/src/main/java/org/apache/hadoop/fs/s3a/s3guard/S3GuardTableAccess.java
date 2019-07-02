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

package org.apache.hadoop.fs.s3a.s3guard;

import java.util.Collection;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.ScanExpressionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.VERSION_MARKER;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.CHILD;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.PARENT;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.TABLE_VERSION;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.itemToPathMetadata;

/**
 * Package-scoped accessor to table state in S3Guard.
 * This is for maintenance, diagnostics and testing: it is <i>not</i> to
 * be used otherwise.
 * <ol>
 *   <li>
 *     Some of the operations here may dramatically alter the state of
 *     a table, so use carefully.
 *   </li>
 *   <li>
 *     Operations to assess consistency of a store are best executed
 *     against a table which is otherwise inactive.
 *   </li>
 *   <li>
 *     No retry/throttling or AWS to IOE logic here.
 *   </li>
 *   <li>
 *     If a scan or query includes the version marker in the result, it
 *     is converted to a VersionMarker instance.
 *   </li>
 * </ol>
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class S3GuardTableAccess {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3GuardTableAccess.class);

  private final DynamoDBMetadataStore store;

  private final Table table;

  S3GuardTableAccess(final DynamoDBMetadataStore store) {
    this.store = checkNotNull(store);
    this.table = store.getTable();
  }

  private String getUsername() {
    return store.getUsername();
  }


  ItemCollection<QueryOutcome> query(QuerySpec spec) {
    return table.query(spec);
  }

  Iterable<DDBPathMetadata> queryMetadata(QuerySpec spec) {
    return new DDBPathMetadataCollection<>(query(spec));
  }

  ItemCollection<ScanOutcome> scan(ExpressionSpecBuilder spec) {
    return table.scan(spec.buildForScan());
  }

  Iterable<DDBPathMetadata> scanMetadata(ExpressionSpecBuilder spec) {
    return new DDBPathMetadataCollection<>(scan(spec));
  }

  void delete(Collection<Path> paths) {
    paths.stream()
        .map(PathMetadataDynamoDBTranslation::pathToKey)
        .forEach(table::deleteItem);
  }

  private final class DDBPathMetadataCollection<T>
      implements Iterable<DDBPathMetadata> {

    private final ItemCollection<T> outcome;

    private DDBPathMetadataCollection(final ItemCollection<T> outcome) {
      this.outcome = outcome;
    }

    @Override
    public Iterator<DDBPathMetadata> iterator() {
      return new DDBPathMetadataIterator<>(outcome.iterator());
    }

  }

  private final class DDBPathMetadataIterator<T> implements
      Iterator<DDBPathMetadata> {

    private final IteratorSupport<Item, T> it;

    private DDBPathMetadataIterator(final IteratorSupport<Item, T> it) {
      this.it = it;
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public DDBPathMetadata next() {
      Item item = it.next();
      Pair<String, String> key = primaryKey(item);
      if (VERSION_MARKER.equals(key.getLeft()) &&
          VERSION_MARKER.equals(key.getRight())) {
        // a version marker is found, return the special type
        return new VersionMarker(item);
      } else {
        return itemToPathMetadata(item, getUsername());
      }
    }

  }

  static final class VersionMarker extends DDBPathMetadata {

    VersionMarker(Item versionMarker) {
      super(new S3AFileStatus(true, new Path("/VERSION"),
          "" + versionMarker.getString(TABLE_VERSION)));
    }
  }

  private static Pair<String, String> primaryKey(Item it) {
    return Pair.of(it.getString(PARENT), it.getString(CHILD));
  }
}
