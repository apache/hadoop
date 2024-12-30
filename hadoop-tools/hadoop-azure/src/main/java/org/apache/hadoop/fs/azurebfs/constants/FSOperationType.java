/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"), you may not use this file except in compliance
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

public enum FSOperationType {
    ACCESS("AS"),
    APPEND("AP"),
    BREAK_LEASE("BL"),
    CREATE("CR"),
    CREATE_FILESYSTEM("CF"),
    CREATE_NON_RECURSIVE("CN"),
    DELETE("DL"),
    GET_ACL_STATUS("GA"),
    GET_ATTR("GR"),
    GET_FILESTATUS("GF"),
    LISTSTATUS("LS"),
    MKDIR("MK"),
    MODIFY_ACL("MA"),
    OPEN("OP"),
    HAS_PATH_CAPABILITY("PC"),
    SET_PERMISSION("SP"),
    READ("RE"),
    RELEASE_LEASE("RL"),
    REMOVE_ACL("RA"),
    REMOVE_ACL_ENTRIES("RT"),
    REMOVE_DEFAULT_ACL("RD"),
    RENAME("RN"),
    SET_ATTR("SR"),
    SET_OWNER("SO"),
    SET_ACL("SA"),
    TEST_OP("TS"),
    WRITE("WR");

    private final String opCode;

    FSOperationType(String opCode) {
        this.opCode = opCode;
    }

    @Override
    public String toString() {
        return opCode;
    }
}
