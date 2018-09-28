/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.audit;
/**
 ******************************************************************************
 *                              Important
 * 1. Any changes to classes in this package can render the logging
 * framework broken.
 * 2. The logger framework has been designed keeping in mind future
 * plans to build a log parser.
 * 3. Please exercise great caution when attempting changes in this package.
 ******************************************************************************
 *
 *
 * This package lays the foundation for Audit logging in Ozone.
 * AuditLogging in Ozone has been built using log4j2 which brings in new
 * features that facilitate turning on/off selective audit events by using
 * MarkerFilter, checking for change in logging configuration periodically
 * and reloading the changes, use of disruptor framework for improved
 * Asynchronous logging.
 *
 * The log4j2 configurations can be specified in XML, YAML, JSON and
 * Properties file. For Ozone, we are using the Properties file due to sheer
 * simplicity, readability and ease of modification.
 *
 * log4j2 configuration file can be passed to startup command with option
 * -Dlog4j.configurationFile unlike -Dlog4j.configuration in log4j 1.x
 *
 ******************************************************************************
 *          Understanding the Audit Logging framework in Ozone.
 ******************************************************************************
 * **** Auditable ***
 * This is an interface to mark an entity as auditable.
 * This interface must be implemented by entities requiring audit logging.
 * For example - OMVolumeArgs, OMBucketArgs.
 * The implementing class must override toAuditMap() to return an
 * instance of Map<Key, Value> where both Key and Value are String.
 *
 * Key: must contain printable US ASCII characters
 * May not contain a space, =, ], or "
 * If the key is multi word then use camel case.
 *
 * Value: if it is a collection/array, then it must be converted to a comma
 * delimited string
 *
 * *** AuditAction ***
 * This is an interface to define the various type of actions to be audited.
 * To ensure separation of concern, for each sub-component you must create an
 * Enum to implement AuditAction.
 * Structure of Enum can be referred from the test class DummyAction.
 *
 * For starters, we expect following 3 implementations of AuditAction:
 * OMAction - to define action types for Ozone Manager
 * SCMAction - to define action types for Storage Container manager
 * DNAction - to define action types for Datanode
 *
 * *** AuditEventStatus ***
 * Enum to define Audit event status like success and failure.
 * This is used in AuditLogger.logXXX() methods.
 *
 *  * *** AuditLogger ***
 * This is where the audit logging magic unfolds.
 * The class has 2 Markers defined - READ and WRITE.
 * These markers are used to tag when logging events.
 *
 * *** AuditLoggerType ***
 * Enum to define the various AuditLoggers in Ozone
 *
 * *** AuditMarker ***
 * Enum to define various Audit Markers used in AuditLogging.
 *
 * *** AuditMessage ***
 * Entity to define an audit message to be logged
 * It will generate a message formatted as:
 * user=xxx ip=xxx op=XXXX_XXXX {key=val, key1=val1..} ret=XXXXXX
 *
 * *** Auditor ***
 * Interface to mark an actor class as Auditor
 * Must be implemented by class where we want to log audit events
 * Implementing class must override and implement methods
 * buildAuditMessageForSuccess and buildAuditMessageForFailure.
 *
 * ****************************************************************************
 *                              Usage
 * ****************************************************************************
 * Using the AuditLogger to log events:
 * 1. Get a logger by specifying the appropriate logger type
 * Example: ExtendedLogger AUDIT = new AuditLogger(AuditLoggerType.OMLogger)
 *
 * 2. Construct an instance of AuditMessage
 *
 * 3. Log Read/Write and Success/Failure event as needed.
 * Example
 * AUDIT.logWriteSuccess(buildAuditMessageForSuccess(params))
 *
 * 4. Log Level implicitly defaults to INFO for xxxxSuccess() and ERROR for
 * xxxxFailure()
 * AUDIT.logWriteSuccess(buildAuditMessageForSuccess(params))
 * AUDIT.logWriteFailure(buildAuditMessageForSuccess(params))
 *
 * See sample invocations in src/test in the following class:
 * org.apache.hadoop.ozone.audit.TestOzoneAuditLogger
 *
 * ****************************************************************************
 *                      Defining new Logger types
 * ****************************************************************************
 * New Logger type can be added with following steps:
 * 1. Update AuditLoggerType to add the new type
 * 2. Create new Enum by implementing AuditAction if needed
 * 3. Ensure the required entity implements Auditable
 *
 * ****************************************************************************
 *                      Defining new Marker types
 * ****************************************************************************
 * New Markers can be configured as follows:
 * 1. Define new markers in AuditMarker
 * 2. Get the Marker in AuditLogger for use in the log methods, example:
 * private static final Marker WRITE_MARKER = AuditMarker.WRITE.getMarker();
 * 3. Define log methods in AuditLogger to use the new Marker type
 * 4. Call these new methods from the required classes to audit with these
 * new markers
 * 5. The marker based filtering can be configured in log4j2 configurations
 * Refer log4j2.properties in src/test/resources for a sample.
 */
