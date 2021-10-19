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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.VERSION_MARKER_ITEM_NAME;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.CHILD;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.PARENT;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.TABLE_VERSION;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.itemToPathMetadata;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.pathToKey;

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
 *     is converted to a {@link VersionMarker} instance.
 *   </li>
 * </ol>
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Retries.OnceRaw
class S3GuardTableAccess {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3GuardTableAccess.class);

  /**
   * Store instance to work with.
   */
  private final DynamoDBMetadataStore store;

  /**
   * Table; retrieved from the store.
   */
  private final Table table;

  /**
   * Construct.
   * @param store store to work with.
   */
  S3GuardTableAccess(final DynamoDBMetadataStore store) {
    this.store = checkNotNull(store);
    this.table = checkNotNull(store.getTable());
  }

  /**
   * Username of user in store.
   * @return a string.
   */
  private String getUsername() {
    return store.getUsername();
  }

  /**
   * Execute a query.
   * @param spec query spec.
   * @return the outcome.
   */
  @Retries.OnceRaw
  ItemCollection<QueryOutcome> query(QuerySpec spec) {
    return table.query(spec);
  }

  /**
   * Issue a query where the result is to be an iterator over
   * the entries
   * of DDBPathMetadata instances.
   * @param spec query spec.
   * @return an iterator over path entries.
   */
  @Retries.OnceRaw
  Iterable<DDBPathMetadata> queryMetadata(QuerySpec spec) {
    return new DDBPathMetadataCollection<>(query(spec));
  }

  @Retries.OnceRaw
  ItemCollection<ScanOutcome> scan(ExpressionSpecBuilder spec) {
    return table.scan(spec.buildForScan());
  }

  @Retries.OnceRaw
  Iterable<DDBPathMetadata> scanMetadata(ExpressionSpecBuilder spec) {
    return new DDBPathMetadataCollection<>(scan(spec));
  }

  @Retries.OnceRaw
  void delete(Collection<Path> paths) {
    paths.stream()
        .map(PathMetadataDynamoDBTranslation::pathToKey)
        .forEach(table::deleteItem);
  }

  @Retries.OnceRaw
  void delete(Path path) {
    table.deleteItem(pathToKey(path));
  }

  /**
   * A collection which wraps the result of a query or scan.
   * Important: iterate through this only once; the outcome
   * of repeating an iteration is "undefined"
   * @param <T> type of outcome.
   */
  private final class DDBPathMetadataCollection<T>
      implements Iterable<DDBPathMetadata> {

    /**
     * Query/scan result.
     */
    private final ItemCollection<T> outcome;

    /**
     * Instantiate.
     * @param outcome query/scan outcome.
     */
    private DDBPathMetadataCollection(final ItemCollection<T> outcome) {
      this.outcome = outcome;
    }

    /**
     * Get the iterator.
     * @return the iterator.
     */
    @Override
    public Iterator<DDBPathMetadata> iterator() {
      return new DDBPathMetadataIterator<>(outcome.iterator());
    }

  }

  /**
   * An iterator which converts the iterated-over result of
   * a query or scan into a {@code DDBPathMetadataIterator} entry.
   * @param <T> type of source.
   */
  private final class DDBPathMetadataIterator<T> implements
      Iterator<DDBPathMetadata> {

    /**
     * Iterator to invoke.
     */
    private final IteratorSupport<Item, T> it;

    /**
     * Instantiate.
     * @param it Iterator to invoke.
     */
    private DDBPathMetadataIterator(final IteratorSupport<Item, T> it) {
      this.it = it;
    }

    @Override
    @Retries.OnceRaw
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    @Retries.OnceRaw
    public DDBPathMetadata next() {
      Item item = it.next();
      Pair<String, String> key = primaryKey(item);
      if (VERSION_MARKER_ITEM_NAME.equals(key.getLeft()) &&
          VERSION_MARKER_ITEM_NAME.equals(key.getRight())) {
        // a version marker is found, return the special type
        return new VersionMarker(item);
      } else {
        return itemToPathMetadata(item, getUsername());
      }
    }

  }

  /**
   * DDBPathMetadata subclass returned when a query returns
   * the version marker.
   * There is a FileStatus returned where the owner field contains
   * the table version; the path is always the unqualified path "/VERSION".
   * Because it is unqualified, operations which treat this as a normal
   * DDB metadata entry usually fail.
   */
  static final class VersionMarker extends DDBPathMetadata {

    /**
     * Instantiate.
     * @param versionMarker the version marker.
     */
    VersionMarker(Item versionMarker) {
      super(new S3AFileStatus(true, new Path("/VERSION"),
          "" + versionMarker.getString(TABLE_VERSION)));
    }
  }

  /**
   * Given an item, split it to the parent and child fields.
   * @param item item to split.
   * @return (parent, child).
   */
  private static Pair<String, String> primaryKey(Item item) {
    return Pair.of(item.getString(PARENT), item.getString(CHILD));
  }
}
