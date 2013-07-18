# Copyright (c) 2004-2013 GoPivotal, Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
# -----
# This module performs a simple store dump, and demonstrates basic querying options.

# Resolve dependencies.
require 'rubygems'
require 'affinity'

# Connect to the Affinity server.
Affinity::Connection.open({:host=>"localhost", :port=>4560, :owner=>"rubytests", :pw=>nil}) do |lAffinity|
  # Define the parameters of the query.
  lQuery = "SELECT *"
  lCount = lAffinity.qCount lQuery
  puts "TOTAL COUNT: #{lCount}"
  sleep(1)
  lPageSize = 200
  lProtoOut = true
  # Go.
  lOffset = 0
  while lOffset < lCount
    lOptions = {:limit=>lPageSize, :offset=>lOffset}
    if lProtoOut # Protobuf output.
      Affinity::PIN.loadPINs(lAffinity.qProto(lQuery, lOptions)).each do |iP|
        puts "#{iP.inspect}"
      end
    else # JSON output.
      # Review: Currently the ruby JSON parser seems to choke on class creation results; curiously, I'm not seeing this in 'irb'; maybe a version-related issue.
      puts "#{lAffinity.q(lQuery, lOptions).inspect}"
    end
    lOffset = lOffset + lPageSize
  end
end
