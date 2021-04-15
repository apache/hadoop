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

package org.apache.hadoop.fs.azurebfs.constants;

public final class HdfsOperationConstants {
    public static final String ACCESS = "AS";
    public static final String ACQUIRE_LEASE = "AL";
    public static final String APPEND = "AP";
    public static final String BREAK_LEASE = "BL";
    public static final String CREATE = "CR";
    public static final String CREATE_FILESYSTEM = "CF";
    public static final String DELETE = "DL";
    public static final String GET_ACL_STATUS = "GA";
    public static final String GET_ATTR = "GR";
    public static final String GET_FILESTATUS = "GF";
    public static final String LISTSTATUS = "LS";
    public static final String MKDIR = "MK";
    public static final String MODIFY_ACL = "MA";
    public static final String OPEN = "OP";
    public static final String HAS_PATH_CAPABILITY = "PC";
    public static final String SET_PERMISSION = "SP";
    public static final String READ = "RE";
    public static final String RELEASE_LEASE = "RL";
    public static final String REMOVE_ACL = "RA";
    public static final String REMOVE_ACL_ENTRIES = "RT";
    public static final String REMOVE_DEFAULT_ACL = "RD";
    public static final String RENAME = "RN";
    public static final String SET_ATTR = "SR";
    public static final String SET_OWNER = "SO";
    public static final String SET_ACL = "SA";
    public static final String TEST_OP = "TS";
    public static final String WRITE = "WR";

    private HdfsOperationConstants() {}
}
