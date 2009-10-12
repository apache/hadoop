# Copyright 2009 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TableIndexed.rb
# Extends HBase shell with operations on IndexedTables.

# Usage: within the HBase shell, load 'TableIndexed.rb'. Transactional
# jar must be in the classpath.

import org.apache.hadoop.hbase.client.tableindexed.IndexedTableAdmin
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification

# Creates an index using the supplied index specification.
# [table_name] the name of the table to index.
# [index_spec] the IndexSpecification describing the index wanted.
def create_index(table_name, index_spec)
  @iadmin ||= IndexedTableAdmin.new(@configuration)
  @iadmin.addIndex(table_name.to_java_bytes, index_spec)
end

# Creates an index for a field guaranteed to have unique values. If
# application code does not ensure uniqueness, behavior is undefined.
# [table_name] the name of the table to index.
# [index_name] the name of the index.
# [column] the column name to be indexed, must respond_to to_java_bytes.
def create_unique_index(table_name, index_name, column)
  spec = IndexSpecification.for_unique_index(index_name, column.to_java_bytes)
  create_index(table_name, spec)
end

# Creates an index using the standard simple index key. Supports one
# to many mappings from indexed values to rows in the primary table.
# [table_name] the name of the table to index.
# [index_name] the name of the index.
# [column] the column name to be indexed, must respond_to to_java_bytes.
def create_simple_index(table_name, index_name, column)
  spec = new IndexSpecification(index_name, column.to_java_bytes)
  create_index(table_name, spec)
end
