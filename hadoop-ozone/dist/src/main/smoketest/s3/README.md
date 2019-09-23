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

## Ozone S3 Gatway Acceptance Tests

Note: the aws cli based acceptance tests can be cross-checked with the original AWS s3 endpoint.

You need to

  1. Create a bucket
  2. Configure your local aws cli
  3. Set bucket/endpointurl during the robot test execution

```
robot -v bucket:ozonetest -v OZONE_TEST:false -v OZONE_S3_SET_CREDENTIALS:false -v ENDPOINT_URL:https://s3.us-east-2.amazonaws.com smoketest/s3
```
