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
package org.apache.hadoop.fs.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.functional.Tuples;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.BulkDeleteUtils.validateBulkDeletePaths;

/**
 * Default implementation of the {@link BulkDelete} interface.
 */
public class DefaultBulkDeleteOperation implements BulkDelete {

    private static Logger LOG = LoggerFactory.getLogger(DefaultBulkDeleteOperation.class);

    /** Default page size for bulk delete. */
    private static final int DEFAULT_PAGE_SIZE = 1;

    /** Base path for the bulk delete operation. */
    private final Path basePath;

    /** Delegate File system make actual delete calls. */
    private final FileSystem fs;

    public DefaultBulkDeleteOperation(Path basePath,
                                      FileSystem fs) {
        this.basePath = requireNonNull(basePath);
        this.fs = fs;
    }

    @Override
    public int pageSize() {
        return DEFAULT_PAGE_SIZE;
    }

    @Override
    public Path basePath() {
        return basePath;
    }

    /**
     * {@inheritDoc}.
     * The default impl just calls {@code FileSystem.delete(path, false)}
     * on the single path in the list.
     */
    @Override
    public List<Map.Entry<Path, String>> bulkDelete(Collection<Path> paths)
            throws IOException, IllegalArgumentException {
        validateBulkDeletePaths(paths, DEFAULT_PAGE_SIZE, basePath);
        List<Map.Entry<Path, String>> result = new ArrayList<>();
        if (!paths.isEmpty()) {
            // As the page size is always 1, this should be the only one
            // path in the collection.
            Path pathToDelete = paths.iterator().next();
            try {
                fs.delete(pathToDelete, false);
            } catch (IOException ex) {
                LOG.debug("Couldn't delete {} - exception occurred: {}", pathToDelete, ex);
                result.add(Tuples.pair(pathToDelete, ex.toString()));
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {

    }
}
