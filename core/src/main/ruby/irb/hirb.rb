#
# Copyright 2010 The Apache Software Foundation
#
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
#

module IRB
  # Subclass of IRB so can intercept methods
  class HIRB < Irb
    def initialize
      # This is ugly.  Our 'help' method above provokes the following message
      # on irb construction: 'irb: warn: can't alias help from irb_help.'
      # Below, we reset the output so its pointed at /dev/null during irb
      # construction just so this message does not come out after we emit
      # the banner.  Other attempts at playing with the hash of methods
      # down in IRB didn't seem to work. I think the worst thing that can
      # happen is the shell exiting because of failed IRB construction with
      # no error (though we're not blanking STDERR)
      begin
        f = File.open("/dev/null", "w")
        $stdout = f
        super
      ensure
        f.close()
        $stdout = STDOUT
      end
    end

    def output_value
      # Suppress output if last_value is 'nil'
      # Otherwise, when user types help, get ugly 'nil'
      # after all output.
      if @context.last_value != nil
        super
      end
    end
  end
end
