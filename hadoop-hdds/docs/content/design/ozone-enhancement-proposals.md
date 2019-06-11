---
title: Ozone Enhancement Proposals
summary: Definition of the process to share new technical proposals with the Ozone community.
date: 2019-06-07
jira: HDDS-1659
status: current
author: Anu Enginner, Marton Elek
---

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

## Proposed solution

 * Open a dedicated Jira (`HDDS-*` but with specific component)
 * Use standard name prefix in the jira (easy to filter on the mailing list) `[OEP]
 * Create a PR to merge the design doc (markdown) to `hadoop-hdds/docs/content/proposal` (will be part of the docs)
 * Discuss it as before (lazy consesus, except if somebody calls for a real vote)
 * Design docs can be updated according to the changes during the implementation

## Document template

This the proposed template to document any proposal. It's recommended but not required the use exactly the some structure. Some proposal may require different structure, but we need the following information.

1. Summary

> Give a one sentence summary, like the jira title. It will be displayed on the documentation page. Should be enough to understand

2. Problem statement (Motivation / Abstract)

> What is the problem and how would you solve it? Think about an abstract of a paper: one paragraph overview. Why will the world better with this change?

3. Non-goals

 > Very important to define what is outside of the scope of this proposal

4.   Technical Description (Architecture and implementation details)

 > Explain the problem in more details. How can it be reproduced? What is the current solution? What is the limitation of the current solution?

 > How the new proposed solution would solve the problem? Architectural design.

 > Implementation details. What should be changed in the code. Is it a huge change? Do we need to change wire protocol? Backward compatibility?

5. Alternatives

 > What are the other alternatives you considered and why do yoy prefer the proposed solution The goal of this section is to help people understand why this is the best solution now, and also to prevent churn in the future when old alternatives are reconsidered.

Note: In some cases 4/5 can be combined. For example if you have multiple proposals, the first version may include multiple solutions. At the end ot the discussion we can move the alternatives to 5. and explain why the community is decided to use the selected option.

6. Plan

 > Planning to implement the feature. Estimated size of the work? Do we need feature branch? Any migration plan, dependency? If it's not a big new feature it can be one sentence or optional.

7. References

## Workflows form other projects

There are similar process in other open source projects. This document and the template is inspired by the following projects:

 * [Apache Kafka Improvement Proposals](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals)
 * [Apache Spark Project Improvement Proposals](https://spark.apache.org/improvement-proposals.html)
 * [Kubernetes Enhancement Proposals](https://github.com/kubernetes/enhancements/tree/master/keps)

Short summary if the porcesses:

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

