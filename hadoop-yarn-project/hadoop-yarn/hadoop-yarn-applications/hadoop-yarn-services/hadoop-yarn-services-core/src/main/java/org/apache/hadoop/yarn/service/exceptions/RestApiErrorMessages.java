/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.exceptions;

public interface RestApiErrorMessages {
  String ERROR_APPLICATION_NAME_INVALID =
      "Service name is either empty or not provided";
  String ERROR_APPLICATION_VERSION_INVALID =
      "Version of service %s is either empty or not provided";
  String ERROR_APPLICATION_NAME_INVALID_FORMAT =
      "Service name %s is not valid - only lower case letters, digits, " +
          "and hyphen are allowed, and the name must be no more " +
          "than 63 characters";
  String ERROR_COMPONENT_NAME_INVALID =
      "Component name must be no more than %s characters: %s";
  String ERROR_COMPONENT_NAME_CONFLICTS_WITH_SERVICE_NAME =
      "Component name %s must not be same as service name %s";
  String ERROR_USER_NAME_INVALID =
      "User name must be no more than 63 characters";

  String ERROR_APPLICATION_NOT_RUNNING = "Service not running";
  String ERROR_APPLICATION_DOES_NOT_EXIST = "Service not found";
  String ERROR_APPLICATION_IN_USE = "Service already exists in started"
      + " state";
  String ERROR_APPLICATION_INSTANCE_EXISTS = "Service already exists in"
      + " stopped/failed state (either restart with PUT or destroy with DELETE"
      + " before creating a new one)";

  String ERROR_SUFFIX_FOR_COMPONENT =
      " for component %s (nor at the global level)";
  String ERROR_ARTIFACT_INVALID = "Artifact is not provided";
  String ERROR_ARTIFACT_FOR_COMP_INVALID =
      ERROR_ARTIFACT_INVALID + ERROR_SUFFIX_FOR_COMPONENT;
  String ERROR_ARTIFACT_ID_INVALID =
      "Artifact id (like docker image name) is either empty or not provided";
  String ERROR_ARTIFACT_ID_FOR_COMP_INVALID =
      ERROR_ARTIFACT_ID_INVALID + ERROR_SUFFIX_FOR_COMPONENT;
  String ERROR_ARTIFACT_PATH_FOR_COMP_INVALID = "For component %s with %s "
      + "artifact, path does not exist: %s";
  String ERROR_CONFIGFILE_DEST_FILE_FOR_COMP_NOT_ABSOLUTE = "For component %s "
      + "with %s artifact, dest_file must be a relative path: %s";

  String ERROR_RESOURCE_INVALID = "Resource is not provided";
  String ERROR_RESOURCE_FOR_COMP_INVALID =
      ERROR_RESOURCE_INVALID + ERROR_SUFFIX_FOR_COMPONENT;
  String ERROR_RESOURCE_MEMORY_INVALID =
      "Service resource or memory not provided";
  String ERROR_RESOURCE_CPUS_INVALID =
      "Service resource or cpus not provided";
  String ERROR_RESOURCE_CPUS_INVALID_RANGE =
      "Unacceptable no of cpus specified, either zero or negative";
  String ERROR_RESOURCE_MEMORY_FOR_COMP_INVALID =
      ERROR_RESOURCE_MEMORY_INVALID + ERROR_SUFFIX_FOR_COMPONENT;
  String ERROR_RESOURCE_CPUS_FOR_COMP_INVALID =
      ERROR_RESOURCE_CPUS_INVALID + ERROR_SUFFIX_FOR_COMPONENT;
  String ERROR_RESOURCE_CPUS_FOR_COMP_INVALID_RANGE =
      ERROR_RESOURCE_CPUS_INVALID_RANGE
          + " for component %s (or at the global level)";
  String ERROR_CONTAINERS_COUNT_INVALID =
      "Invalid no of containers specified";
  String ERROR_CONTAINERS_COUNT_FOR_COMP_INVALID =
      ERROR_CONTAINERS_COUNT_INVALID + ERROR_SUFFIX_FOR_COMPONENT;
  String ERROR_DEPENDENCY_INVALID = "Dependency %s for component %s is " +
      "invalid, does not exist as a component";
  String ERROR_DEPENDENCY_CYCLE = "Invalid dependencies, a cycle may " +
      "exist: %s";

  String ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_NOT_SUPPORTED =
      "Cannot specify" + " cpus/memory along with profile";
  String ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_FOR_COMP_NOT_SUPPORTED =
      ERROR_RESOURCE_PROFILE_MULTIPLE_VALUES_NOT_SUPPORTED
          + " for component %s";
  String ERROR_RESOURCE_PROFILE_NOT_SUPPORTED_YET =
      "Resource profile is not " + "supported yet. Please specify cpus/memory.";

  String ERROR_NULL_ARTIFACT_ID =
      "Artifact Id can not be null if artifact type is none";
  String ERROR_ABSENT_NUM_OF_INSTANCE =
      "Num of instances should appear either globally or per component";
  String ERROR_ABSENT_LAUNCH_COMMAND =
      "launch_command is required when type is not DOCKER";

  String ERROR_QUICKLINKS_FOR_COMP_INVALID = "Quicklinks specified at"
      + " component level, needs corresponding values set at service level";
  // Note: %sin is not a typo. Constraint name is optional so the error messages
  // below handle that scenario by adding a space if name is specified.
  String ERROR_PLACEMENT_POLICY_CONSTRAINT_TYPE_NULL = "Type not specified "
      + "for constraint %sin placement policy of component %s.";
  String ERROR_PLACEMENT_POLICY_CONSTRAINT_SCOPE_NULL = "Scope not specified "
      + "for constraint %sin placement policy of component %s.";
  String ERROR_PLACEMENT_POLICY_CONSTRAINT_TAGS_NULL = "Tag(s) not specified "
      + "for constraint %sin placement policy of component %s.";
  String ERROR_PLACEMENT_POLICY_TAG_NAME_NOT_SAME = "Invalid target tag %s "
      + "specified in placement policy of component %s. For now, target tags "
      + "support self reference only. Specifying anything other than its "
      + "component name is not supported. Set target tag of component %s to "
      + "%s.";
  String ERROR_PLACEMENT_POLICY_TAG_NAME_INVALID = "Invalid target tag %s "
      + "specified in placement policy of component %s. Target tags should be "
      + "a valid component name in the service.";
  String ERROR_PLACEMENT_POLICY_EXPRESSION_ELEMENT_NAME_INVALID = "Invalid "
      + "expression element name %s specified in placement policy of component "
      + "%s. Expression element names should be a valid constraint name or an "
      + "expression name defined for this component only.";
  String ERROR_KEYTAB_URI_SCHEME_INVALID = "Unsupported keytab URI scheme: %s";
  String ERROR_KEYTAB_URI_INVALID = "Invalid keytab URI: %s";

  String ERROR_COMP_INSTANCE_DOES_NOT_NEED_UPGRADE = "The component instance " +
      "(%s) does not need an upgrade.";

  String ERROR_COMP_DOES_NOT_NEED_UPGRADE = "The component (%s) does not need" +
      " an upgrade.";
  String ERROR_KERBEROS_PRINCIPAL_NAME_FORMAT = "Kerberos principal (%s) does " +
      " not contain a hostname.";
  String ERROR_KERBEROS_PRINCIPAL_MISSING = "Kerberos principal or keytab is" +
      " missing.";
}
