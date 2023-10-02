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

# S3 Select

S3 Select is a feature for Amazon S3 introduced in April 2018. It allows for
SQL-like SELECT expressions to be applied to files in some structured
formats, including CSV and JSON.

It is no longer supported in Hadoop releases.

Any Hadoop release built on the [AWS V2 SDK](./aws_sdk_upgrade.html)
will reject calls to open files using the select APIs.

If a build of Hadoop with S3 Select is desired, the relevant
classes can be found in hadoop trunk commit `8bf72346a59c`.