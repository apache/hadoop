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


export interface PolicyDescription {
  id: string;
  name: string;
  description: string;
  behavior: string;
  example?: string;
  note?: string;
}

export const POLICY_DESCRIPTIONS: PolicyDescription[] = [
  {
    id: 'specified',
    name: 'Specified Queue',
    description: 'Places the application to the queue that was defined during submission.',
    behavior: 'the application will be placed to the queue that was defined during submission',
    example: 'If an application is submitted to queue "root.production", it will be placed there.',
  },
  {
    id: 'reject',
    name: 'Reject Application',
    description: 'Rejects the submission.',
    behavior: 'the submission will be rejected',
    example: 'Applications matching this rule will be rejected and not run.',
  },
  {
    id: 'defaultQueue',
    name: 'Default Queue',
    description:
      'Places the application into the default queue root.default or to its overwritten value set by setDefaultQueue.',
    behavior:
      'the application will be placed into the default queue root.default or to its overwritten value',
    example: 'Applications will be placed in "root.default" unless changed by a previous rule.',
  },
  {
    id: 'user',
    name: 'User Queue',
    description: 'Places the application into a queue which matches the username of the submitter.',
    behavior:
      'the application will be placed into a queue which matches the username of the submitter',
    example: 'User "alice" submitting an application will have it placed in queue "alice".',
  },
  {
    id: 'applicationName',
    name: 'Application Name',
    description: 'Places the application into a queue which matches the name of the application.',
    behavior:
      'the application will be placed into a queue which matches the name of the application',
    note: 'Important: it is case-sensitive, white spaces are not removed.',
    example: 'Application named "spark-job" will be placed in queue "spark-job".',
  },
  {
    id: 'primaryGroup',
    name: 'Primary Group',
    description:
      'Places the application into a queue which matches the primary group of the submitter.',
    behavior:
      'the application will be placed into a queue which matches the primary group of the submitter',
    example:
      'User with primary group "developers" will have applications placed in queue "developers".',
  },
  {
    id: 'primaryGroupUser',
    name: 'Primary Group → User',
    description:
      'Places the application into the queue hierarchy root.[parentQueue].<primaryGroup>.<userName>.',
    behavior:
      'the application will be placed into the queue hierarchy root.[parentQueue].<primaryGroup>.<userName>',
    note: 'Note that parentQueue is optional.',
    example:
      'User "alice" with primary group "dev" will be placed in "root.dev.alice" (or "root.users.dev.alice" if parentQueue is "root.users").',
  },
  {
    id: 'secondaryGroup',
    name: 'Secondary Group',
    description:
      'Places the application into a queue which matches the secondary group of the submitter.',
    behavior:
      'the application will be placed into a queue which matches the secondary group of the submitter',
    example:
      'User with secondary group "analytics" will have applications placed in queue "analytics".',
  },
  {
    id: 'secondaryGroupUser',
    name: 'Secondary Group → User',
    description:
      'Places the application into the queue hierarchy root.[parentQueue].<secondaryGroup>.<userName>.',
    behavior:
      'the application will be placed into the queue hierarchy root.[parentQueue].<secondaryGroup>.<userName>',
    note: 'Note that parentQueue is optional.',
    example:
      'User "bob" with secondary group "qa" will be placed in "root.qa.bob" (or "root.teams.qa.bob" if parentQueue is "root.teams").',
  },
  {
    id: 'setDefaultQueue',
    name: 'Set as Default Queue',
    description:
      'Changes the default queue from root.default. The change is permanent in a sense that it is not restored in the next rule.',
    behavior: 'changes the default queue from root.default for subsequent rules',
    note: 'You can change the default queue at any point and as many times as necessary.',
    example:
      'Setting default to "root.batch" means subsequent rules using "defaultQueue" policy will use "root.batch".',
  },
  {
    id: 'custom',
    name: 'Custom Placement',
    description: 'Enables the user to use custom placement strings with variables.',
    behavior: 'the application will be placed according to the custom placement string',
    note: 'The engine does only minimal verification - it is your responsibility to provide the correct string.',
    example:
      'Using "%specified.%user.largejobs" to create a hierarchy based on submitted queue and username.',
  },
];

export const CUSTOM_VARIABLES = [
  {
    variable: '%application',
    meaning: 'The name of the submitted application.',
    example: 'For application "MySparkJob", %application = "MySparkJob"',
  },
  {
    variable: '%user',
    meaning: 'The user who submitted the application.',
    example: 'For user "alice", %user = "alice"',
  },
  {
    variable: '%primary_group',
    meaning: 'Primary group of the submitter.',
    example: 'For user with primary group "developers", %primary_group = "developers"',
  },
  {
    variable: '%secondary_group',
    meaning: 'Secondary (supplementary) group of the submitter.',
    example: 'For user with secondary group "analytics", %secondary_group = "analytics"',
  },
  {
    variable: '%default',
    meaning: 'The default queue of the scheduler.',
    example: 'Usually "root.default" unless changed by setDefaultQueue',
  },
  {
    variable: '%specified',
    meaning: 'Contains the queue what the submitter defined.',
    example: 'If submitted to "root.users.mrjobs", %specified = "root.users.mrjobs"',
  },
];

export function getPolicyDescription(policyId: string): PolicyDescription | undefined {
  return POLICY_DESCRIPTIONS.find((p) => p.id === policyId);
}

export const POLICY_DISPLAY_NAMES: Record<string, string> = {
  user: 'User Queue',
  primaryGroup: 'Primary Group',
  primaryGroupUser: 'Primary Group → User',
  secondaryGroup: 'Secondary Group',
  secondaryGroupUser: 'Secondary Group → User',
  specified: 'Specified Queue',
  defaultQueue: 'Default Queue',
  setDefaultQueue: 'Set as Default Queue',
  reject: 'Reject Application',
  custom: 'Custom Placement',
  applicationName: 'Application Name',
};

export function getPolicyDisplayName(policy: string): string {
  return POLICY_DISPLAY_NAMES[policy] || policy;
}
