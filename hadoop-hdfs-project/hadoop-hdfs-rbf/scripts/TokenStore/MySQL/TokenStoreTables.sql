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

-- Script to generate all the tables for the TokenStore in MySQL

USE TokenStore

CREATE TABLE IF NOT EXISTS Tokens(
    sequenceNum int NOT NULL,
    tokenIdentifier varbinary(255) NOT NULL,
    tokenInfo varbinary(255) NOT NULL,
    modifiedTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY(sequenceNum, tokenIdentifier)
);

CREATE TABLE IF NOT EXISTS DelegationKeys(
    keyId int NOT NULL,
    delegationKey varbinary(255) NOT NULL,
    modifiedTime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY(keyId)
);

CREATE TABLE IF NOT EXISTS LastSequenceNum(
    sequenceNum int NOT NULL
);

-- Initialize the LastSequenceNum table with a single entry
INSERT INTO LastSequenceNum (sequenceNum)
SELECT 0 WHERE NOT EXISTS (SELECT * FROM LastSequenceNum);

CREATE TABLE IF NOT EXISTS LastDelegationKeyId(
    keyId int NOT NULL
);

-- Initialize the LastDelegationKeyId table with a single entry
INSERT INTO LastDelegationKeyId (keyId)
SELECT 0 WHERE NOT EXISTS (SELECT * FROM LastDelegationKeyId);
