<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Overview

Yetus helps community driven software projects improve their contribution and release processes by providing:

* A robust system for automatically checking new contributions against a variety of community accepted requirements
* The means to document a well defined supported interface for downstream projects
* Tooling to help release managers generate release documentation based on the information provided by community issue trackers and source repositories

# Yetus Precommit

The Yetus Precommit Patch Tester allows projects to codify their patch acceptance criteria and then evaluate incoming contributions prior to review by a committer.

* Take a quick look at [our glossary of terms](precommit-glossary.md) to ensure you are familiar with the ASF and Maven jargon we'll use as terminology specific to this project.
* For an overview of Yetus' philosophy on testing contributions and how evaluation is performed, see our [overview](precommit-architecture.md).
* To get started on your project, including an explanation of what we'll expect in a runtime environment and what optional utilities we'll leverage, read through the [basic usage guide](precommit-basic.md).
* If your project has advanced requirements such as module relationships not expressed in Maven, special profiles, or a need on os-specific prerequisites not managed by Maven then you'll need to use our [advanced usage guide](precommit-advanced.md).
