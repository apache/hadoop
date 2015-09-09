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

Bug System Support
==================

test-patch has the ability to support multiple bug systems.  Bug tools have some extra hooks to fetch patches, line-level reporting, and posting a final report. Every bug system plug-in must have one line in order to be recognized:

```bash
add_bugsystem <pluginname>
```

* pluginname\_locate\_patch

    - Given input from the user, download the patch if possible.

* pluginname\_determine\_branch

    - Using any heuristics available, return the branch to process, if possible.

* pluginname\_determine\_issue

    - Using any heuristics available, set the issue, bug number, etc, for this bug system, if possible.  This is typically used to fill in supplementary information in the final output table.

* pluginname\_writecomment

    - Given text input, write this output to the bug system as a comment.  NOTE: It is the bug system's responsibility to format appropriately.

* pluginname\_linecomments

    - This function allows for the system to write specific comments on specific lines if the bug system supports code review comments.

* pluginname\_finalreport

    - Write the final result table to the bug system.
