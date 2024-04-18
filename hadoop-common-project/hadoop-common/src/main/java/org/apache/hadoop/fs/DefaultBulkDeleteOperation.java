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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.functional.Tuples;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.BulkDeleteUtils.validateBulkDeletePaths;
import static org.apache.hadoop.util.Preconditions.checkArgument;

/**
 * Default implementation of the {@link BulkDelete} interface.
 */
public class DefaultBulkDeleteOperation implements BulkDelete {

    private final int pageSize;

    private final Path basePath;

    private final FileSystem fs;

    public DefaultBulkDeleteOperation(int pageSize,
                                      Path basePath,
                                      FileSystem fs) {
        checkArgument(pageSize == 1, "Page size must be equal to 1");
        this.pageSize = pageSize;
        this.basePath = requireNonNull(basePath);
        this.fs = fs;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public Path basePath() {
        return basePath;
    }

    @Override
    public List<Map.Entry<Path, String>> bulkDelete(Collection<Path> paths)
            throws IOException, IllegalArgumentException {
        validateBulkDeletePaths(paths, pageSize, basePath);
        List<Map.Entry<Path, String>> result = new ArrayList<>();
        // this for loop doesn't make sense as pageSize must be 1.
        for (Path path : paths) {
            try {
                fs.delete(path, false);
                // What to do if this return false?
                // I think we should add the path to the result list with value "Not Deleted".
            } catch (IOException e) {
                result.add(Tuples.pair(path, e.toString()));
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {

    }
}
