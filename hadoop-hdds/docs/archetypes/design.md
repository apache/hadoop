---
title: "{{ replace .Name "-" " " | title }}"
menu: main
jira: HDDS-XXX
summary: One sentence summary. Will be displayed at the main design doc table.
status: current
author: Your Name

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


This the proposed template to document any proposal. It's recommended but not required the use exactly the some structure. Some proposal may require different structure, but we need the following information.

## Summary

> Give a one sentence summary, like the jira title. It will be displayed on the documentation page. Should be enough to understand

## Problem statement (Motivation / Abstract)

> What is the problem and how would you solve it? Think about an abstract of a paper: one paragraph overview. Why will the world better with this change?

## Non-goals

 > Very important to define what is outside of the scope of this proposal

##   Technical Description (Architecture and implementation details)

 > Explain the problem in more details. How can it be reproduced? What is the current solution? What is the limitation of the current solution?

 > How the new proposed solution would solve the problem? Architectural design.

 > Implementation details. What should be changed in the code. Is it a huge change? Do we need to change wire protocol? Backward compatibility?

## Alternatives

 > What are the other alternatives you considered and why do yoy prefer the proposed solution The goal of this section is to help people understand why this is the best solution now, and also to prevent churn in the future when old alternatives are reconsidered.

Note: In some cases 4/5 can be combined. For example if you have multiple proposals, the first version may include multiple solutions. At the end ot the discussion we can move the alternatives to 5. and explain why the community is decided to use the selected option.

## Implementation plan

 > Planning to implement the feature. Estimated size of the work? Do we need feature branch? Any migration plan, dependency? If it's not a big new feature it can be one sentence or optiona;.

## References