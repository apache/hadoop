# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Append str to each item in the given list.
# replaceable with list(TRANSFORM var APPEND ${str}) once we have cmake 3.10.
function (appendToEach var str)
  set(_result)
  foreach (_elem ${${var}})
    list(APPEND _result "${_elem}${str}")
  endforeach ()
  set(${var} ${_result} PARENT_SCOPE)
endfunction (appendToEach)
