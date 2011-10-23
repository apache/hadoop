# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set up sample data for IndexBuilder example
create "people", "attributes"
create "people-email", "INDEX"
create "people-phone", "INDEX"
create "people-name", "INDEX"

[["1", "jenny", "jenny@example.com", "867-5309"],
 ["2", "alice", "alice@example.com", "555-1234"],
 ["3", "kevin", "kevinpet@example.com", "555-1212"]].each do |fields|
  (id, name, email, phone) = *fields
  put "people", id, "attributes:name", name
  put "people", id, "attributes:email", email
  put "people", id, "attributes:phone", phone
end
  
