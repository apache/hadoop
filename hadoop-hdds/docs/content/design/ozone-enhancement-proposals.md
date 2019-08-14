---
title: Ozone Enhancement Proposals
summary: Definition of the process to share new technical proposals with the Ozone community.
date: 2019-06-07
jira: HDDS-1659
status: accepted
author: Anu Enginner, Marton Elek
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## Problem statement

Some of the biggers features requires well defined plans before the implementation. Until now it was managed by uploading PDF design docs to selected JIRA. There are multiple problems with the current practice.

 1. There is no easy way to find existing up-to-date and outdated design docs.
 2. Design docs usually have better description of the problem that the user docs
 3. We need better tools to discuss the design docs in the development phase of the doc

We propose to follow the same process what we have now, but instead of uploading a PDF to the JIRA, create a PR to merge the proposal document to the documentation project.

## Non-goals

 * Modify the existing workflow or approval process
 * Migrate existing documents
 * Make it harder to create design docs (it should be easy to support the creation of proposals for any kind of tasks)
 * Define how the design docs are handled/created *before* the publication (this proposal is about the publishing process)

## Proposed solution

 * Open a dedicated Jira (`HDDS-*` but with specific component)
 * Use standard name prefix in the jira (easy to filter on the mailing list) `[OEP]
 * Create a PR to add the design doc to the current documentation
   * The content of the design can be added to the documentation (Recommended)
   * Or can be added as external reference
 * The design doc (or the summary with the reference) will be merged to the design doc folder of `hadoop-hdds/docs/content/design` (will be part of the docs)
 * Discuss it as before (lazy consesus, except if somebody calls for a real vote)
 * Design docs can be updated according to the changes during the implementation
 * Only the implemented design docs will be visible as part of the design docs


As a result all the design docs can be listed under the documentation page.

A good design doc has the following properties:

 1. Publicly available for anybody (Please try to avoid services which are available only with registration, eg: google docs)
 2. Archived for the future (Commit it to the source OR use apache jira or wiki)
 3. Editable later (Best format is markdown, RTF is also good. PDF has a limitation, it's very hard to reuse the text, or create an updated design doc)
 4. Well structured to make it easy to comment any part of the document (Markdown files which are part of the pull request can be commented in the PR line by line)


### Example 1: Design doc as a markdown file

The easiest way to create a design doc is to create a new markdown file in a PR and merge it to `hadoop-hdds/docs/content/design`.

 1. Publicly available: YES, it can be linked from Apache git or github
 2. Archived: YES, and it's also versioned. All the change history can be tracked.
 3. Editable later: YES, as it's just a simple text file
 4. Commentable: YES, comment can be added to each line.

### Example 2: Design doc as a PDF

A very common practice of today is to create design doc on google docs and upload it to the JIRA.

 1. Publicy available: YES, anybody can download it from the Jira.
 2. Archived: YES, it's available from Apache infra.
 3. Editable: NO, It's harder to reuse the text to import to the docs or create a new design doc.
 4. Commentable: PARTIAL, Not as easy as a text file or the original google docs, but a good structure with numbered section may help


### The format

While the first version (markdown files) are the most powerful, the second version (the existing practice) is also acceptable. In this case we propose to create a PR with adding a reference page *without* the content but including the link.

For example:

```yaml
---
title: Ozone Security Design
summary: A comprehensive description of the security flow between server and client components.
date: 2018-02-22
jira: HDDS-4
status: implemented
author: Sanjay Radia, Jitendra Pandey, Xiaoyu Yao, Anu Engineer

## Summary

Ozone security model is based on Kerberos and similar to the Hadoop security but some of the parts are improved: for example the SCM works as a Certificate Authority and PKI based solutions are wildely used.

## Reference

For more details please check the (uploaded design doc)[https://issues.apache.org/jira/secure/attachment/12911638/HadoopStorageLayerSecurity.pdf].

```

Obviously with the first approach the design doc itself can be included in this markdown file.

## Migration

It's not a hard requirement to migrate all the design doc. But process is always open:

 1. To create reference pages for any of the old design docs
 2. To migrate any new design docs to markdown formats (by anybody not just by the author)
 3. To update any of the old design docs based on the current state of the code (We have versioning!)

## Document template

This the proposed template to document any proposal. It's recommended but not required the use exactly the some structure. Some proposal may require different structure, but we need the following information.

1. Summary

> Give a one sentence summary, like the jira title. It will be displayed on the documentation page. Should be enough to understand

2. Status

Defined in the markdown header. Proposed statuses:

 * `accepted`: (Use this as by default. If not accapted, won't be merged)

 * `implemented`: The discussed technical solution is implemented (maybe with some minor implementation difference)

 * `replaced`: Replaced by a new design doc

 * `outdated`: Code has been changed and design doc doesn't reflect any more the state of the current code.

 Note: the _accepted_ design docs won't be visible as part of the documentation or only under a dedicated section to clearly comminucate that it's not ready, yet.

3. Problem statement (Motivation / Abstract)

> What is the problem and how would you solve it? Think about an abstract of a paper: one paragraph overview. Why will the world better with this change?

4. Non-goals

 > Very important to define what is outside of the scope of this proposal

5.   Technical Description (Architecture and implementation details)

 > Explain the problem in more details. How can it be reproduced? What is the current solution? What is the limitation of the current solution?

 > How the new proposed solution would solve the problem? Architectural design.

 > Implementation details. What should be changed in the code. Is it a huge change? Do we need to change wire protocol? Backward compatibility?

6. Alternatives

 > What are the other alternatives you considered and why do yoy prefer the proposed solution The goal of this section is to help people understand why this is the best solution now, and also to prevent churn in the future when old alternatives are reconsidered.

Note: In some cases 4/5 can be combined. For example if you have multiple proposals, the first version may include multiple solutions. At the end ot the discussion we can move the alternatives to 5. and explain why the community is decided to use the selected option.

7. Plan

 > Planning to implement the feature. Estimated size of the work? Do we need feature branch? Any migration plan, dependency? If it's not a big new feature it can be one sentence or optional.

8. References

## Workflows form other projects

There are similar process in other open source projects. This document and the template is inspired by the following projects:

 * [Apache Kafka Improvement Proposals](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals)
 * [Apache Spark Project Improvement Proposals](https://spark.apache.org/improvement-proposals.html)
 * [Kubernetes Enhancement Proposals](https://github.com/kubernetes/enhancements/tree/master/keps)

Short summary of the processes:

__Kafka__ process:

 * Create wiki page
 * Start discussion on mail thread
 * Vote on mail thread

__Spark__ process:

 * Create JIRA (dedicated label)
 * Discuss on the jira page
 * Vote on dev list

*Kubernetes*:

 * Deditaced git repository
 * KEPs are committed to the repo
 * Well defined approval process managed by SIGs (KEPs are assigned to SIGs)

