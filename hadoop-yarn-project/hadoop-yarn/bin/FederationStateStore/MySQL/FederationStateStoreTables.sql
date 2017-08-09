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

-- Script to generate all the tables for the Federation StateStore in MySQL

USE FederationStateStore

CREATE TABLE applicationsHomeSubCluster(
   applicationId varchar(64) NOT NULL,
   subClusterId varchar(256) NULL,
   CONSTRAINT pk_applicationId PRIMARY KEY (applicationId)
);

CREATE TABLE membership(
   subClusterId varchar(256) NOT NULL,
   amRMServiceAddress varchar(256) NOT NULL,
   clientRMServiceAddress varchar(256) NOT NULL,
   rmAdminServiceAddress varchar(256) NOT NULL,
   rmWebServiceAddress varchar(256) NOT NULL,
   lastHeartBeat datetime NOT NULL,
   state varchar(32) NOT NULL,
   lastStartTime bigint NULL,
   capability varchar(6000),
   CONSTRAINT pk_subClusterId PRIMARY KEY (subClusterId)
);

CREATE TABLE policies(
   queue varchar(256) NOT NULL,
   policyType varchar(256) NOT NULL,
   params varbinary(32768),
   CONSTRAINT pk_queue PRIMARY KEY (queue)
);
