# Copyright (c) 2004-2012 VMware, Inc. All Rights Reserved.
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
# This test file contains a blend of basic tests/samples, meant
# to verify and demonstrate basic Affinity functionality (in ruby).
# Here, the reader can expect to find verifiable answers to many of his basic
# "how to" questions. The file also contains a few longer examples, to give
# a sense of Affinity's typical workflows, in the context of "real" applications.
# It does not replace other existing test suites, such as the kernel test suite
# or the SQL compliance suite (both of which are much more exhaustive).
# But it should constitute a reasonably comprehensive (yet compact)
# first experience of interacting with Affinity in ruby.

# Resolve dependencies.
require 'rubygems'
require 'affinity'
require 'find'

# Connect to the Affinity server.
# Note: Presently, the ruby flavor always uses keep-alive.
Affinity::Connection.open({:host=>"localhost", :port=>4560, :owner=>"rubytests", :pw=>nil}) do |lAffinity|
  lTests =
  [
    #
    # Trivial tests (quick checks / examples).
    #
    {
      :name => "test_basic_pathsql",
      :comment => "Pure pathSQL with json responses.",
      :func => lambda\
      {
        lR1 = lAffinity.q "INSERT (test_basic_pathsql__name, test_basic_pathsql__profession) VALUES ('Roger', 'Painter');"
        lR2 = lAffinity.q "SELECT * WHERE EXISTS(test_basic_pathsql__name);"
        lFound = false
        lR2.each do |iPin| break if lFound = (iPin["id"] == lR1[0]["id"]) end
        raise "Failed to retrieve #{lR1[0]["id"]}" unless lFound
        puts "Done."
      }
    },
    {
      :name => "test_basic_protobuf",
      :comment => "Protobuf-in, pathSQL with protobuf-out, basic PIN interface.",
      :func => lambda\
      {
        lPINs = Affinity::PIN.savePINs([Affinity::PIN[{"test_basic_protobuf__a_string" => "whatever", "test_basic_protobuf__a_number" => 123, "test_basic_protobuf__a_date" => Time.now, "test_basic_protobuf__an_array" => [1, 2, 3, 4]}]])
        puts "created #{lPINs.length} pin: #{lPINs[0].pid}"
        lR1 = Affinity::PIN.loadPINs(lAffinity.qProto "SELECT FROM #{lPINs[0].pid};")
        lAffinity.startTx
        lPINs[0]["test_basic_protobuf__x"] = 202020
        lPINs[0].store("test_basic_protobuf__y", 303030)
        lPINs[0]["test_basic_protobuf__an_array"] << 5
        lAffinity.commitTx
        lR2 = Affinity::PIN.loadPINs(lAffinity.qProto "SELECT WHERE EXISTS(test_basic_protobuf__an_array);")
        puts "retrieved #{lR2.inspect}"
        raise "Failure to push element" unless 5 == lR2[0]["test_basic_protobuf__an_array"].length
        puts "Done."
      }
    },
    #
    # Simple tests for collections (via protobuf).
    # Any property can become a collection.
    # In ruby, collections are exposed with an interface similar to the ruby Array's.
    #
    {
      :name => "test_collections",
      :comment => "Collection-related tests.",
      :func => lambda\
      {
        lNewTest = lambda\
        {
          |name, values|
          pid = Affinity::PIN.savePINs([Affinity::PIN[{name => values}]])[0].pid
          pin = Affinity::PIN.loadPINs(lAffinity.qProto("SELECT * FROM #{pid};"))[0]
          raise "Failure to create or retrieve pin for #{name}" unless pin[name] == values
          pin
        }

        puts "Performing pops."
        lPIN = lNewTest.call("test_collections__v1", 0.upto(7).map{|j| j})
        (lPIN["test_collections__v1"].length - 1).downto(1) do |i|
          raise "Failure to pop" unless lPIN["test_collections__v1"].pop == i
          raise "Failure to update in-memory state after pop" unless lPIN["test_collections__v1"] == 0.upto(i - 1).map{|j| j}
          raise "Failure to update persistent state after pop" unless lPIN.refreshPIN()["test_collections__v1"] == 0.upto(i - 1).map{|j| j}
        end
        lPIN.delete("test_collections__v1")
        raise "Failure to update in-memory state after delete" if lPIN.has_key? "test_collections__v1"
        raise "Failure to update persistent state after delete" if lPIN.refreshPIN().has_key? "test_collections__v1"

        puts "Performing pushes."
        lToPush = [1, 2, "Z", "a", "once upon a time", 2.345, "yes!"]
        lPIN["test_collections__v1"] = [lToPush[0]]
        lToPush.each_index do |i|
          next if i == 0
          raise "Failure to push" unless lPIN["test_collections__v1"].push(lToPush[i]).last == lToPush[i]
          raise "Failure to update in-memory state after push" unless lPIN["test_collections__v1"] == lToPush.slice(0, i + 1)
          raise "Failure to update persistent state after push" unless lPIN.refreshPIN()["test_collections__v1"] == lToPush.slice(0, i + 1)
        end

        puts "Performing inserts."
        lCpy = Array.new(lPIN["test_collections__v1"])
        0.upto(10) do |i|
          at = rand(lCpy.length)
          v = rand()
          lCpy.insert(at, v)
          raise "Failure to insert" unless lPIN["test_collections__v1"].insert(at, v)[at] == v
          raise "Failure to update in-memory state after insert" unless lPIN["test_collections__v1"] == lCpy
          raise "Failure to update persistent state after insert" unless lPIN.refreshPIN()["test_collections__v1"] == lCpy
        end
        lPIN["test_collections__v1"].concat([1, 5, "z", 3.5])
        lCpy.concat([1, 5, "z", 3.5])
        raise "Failure to update in-memory state after concat" unless lPIN["test_collections__v1"] == lCpy
        raise "Failure to update persistent state after concat" unless lPIN.refreshPIN()["test_collections__v1"] == lCpy

        puts "Performing updates."
        0.upto(10) do |i|
          at = rand(lCpy.length)
          v = rand()
          lCpy[at] = v
          raise "Failure to update" unless (lPIN["test_collections__v1"][at] = v) == v
          raise "Failure to update in-memory state after update" unless lPIN["test_collections__v1"] == lCpy
          raise "Failure to update persistent state after update" unless lPIN.refreshPIN()["test_collections__v1"] == lCpy
        end
        lCpy[3..7] = 12345
        lPIN["test_collections__v1"][3..7] = 12345
        raise "Failure to update in-memory state after update range(1)" unless lPIN["test_collections__v1"] == lCpy
        raise "Failure to update persistent state after update range(1)" unless lPIN.refreshPIN()["test_collections__v1"] == lCpy
        lCpy[5, 2] = 54321
        lPIN["test_collections__v1"][5, 2] = 54321
        raise "Failure to update in-memory state after update range(2)" unless lPIN["test_collections__v1"] == lCpy
        raise "Failure to update persistent state after update range(2)" unless lPIN.refreshPIN()["test_collections__v1"] == lCpy

        puts "Performing deletes."
        0.upto(3) do |i|
          at = rand(lCpy.length)
          what = lCpy[at]
          lCpy.delete(what)
          raise "Failure to delete" unless lPIN["test_collections__v1"].delete(what) == what
          raise "Failure to update in-memory state after delete" unless lPIN["test_collections__v1"] == lCpy
          raise "Failure to update persistent state after delete" unless lPIN.refreshPIN()["test_collections__v1"] == lCpy
        end
        0.upto(3) do |i|
          at = rand(lCpy.length)
          what = lCpy[at]
          lCpy.delete_if {|v| v==what}
          lPIN["test_collections__v1"].delete_if {|v| v==what}
          raise "Failure to update in-memory state after delete_if" unless lPIN["test_collections__v1"] == lCpy
          raise "Failure to update persistent state after delete_if" unless lPIN.refreshPIN()["test_collections__v1"] == lCpy
        end
        lP = lNewTest.call("test_collections__v1", 0.upto(50).map{|j| j})
        lPCpy = Array.new(lP["test_collections__v1"])
        lP["test_collections__v1"].slice!(1..10)
        lPCpy.slice!(1..10)
        raise "Failure to update in-memory state after slice!(1)" unless lP["test_collections__v1"] == lPCpy
        raise "Failure to update persistent state after slice!(1)" unless lP.refreshPIN()["test_collections__v1"] == lPCpy
        lP["test_collections__v1"].slice!(5, 15)
        lPCpy.slice!(5, 15)
        raise "Failure to update in-memory state after slice!(2)" unless lP["test_collections__v1"] == lPCpy
        raise "Failure to update persistent state after slice!(2)" unless lP.refreshPIN()["test_collections__v1"] == lPCpy
        lP["test_collections__v1"].slice!(5)
        lPCpy.slice!(5)
        raise "Failure to update in-memory state after slice!(3)" unless lP["test_collections__v1"] == lPCpy
        raise "Failure to update persistent state after slice!(3)" unless lP.refreshPIN()["test_collections__v1"] == lPCpy
        lP = lNewTest.call("test_collections__v1", [1,2,3,4,5,4,3,4,5,6,7,6,5,6,7,8,9,1,2,1,1,1,1,1,2,3,4,11])
        lPCpy = Array.new(lP["test_collections__v1"])
        lP["test_collections__v1"].uniq!
        lPCpy.uniq!
        raise "Failure to update in-memory state after uniq!" unless lP["test_collections__v1"] == lPCpy
        raise "Failure to update persistent state after uniq!" unless lP.refreshPIN()["test_collections__v1"] == lPCpy

        puts "Performing fills."
        lPF = lNewTest.call("test_collections__v2", 0.upto(7).map{|j| j})
        lPFCpy = Array.new(lPF["test_collections__v2"])
        lPF["test_collections__v2"].fill("3.7")
        lPFCpy.fill("3.7")
        raise "Failure to update in-memory state after fill(1)" unless lPF["test_collections__v2"] == lPFCpy
        raise "Failure to update persistent state after fill(1)" unless lPF.refreshPIN()["test_collections__v2"] == lPFCpy
        lPF["test_collections__v2"].fill {|i| 3.7 * i}
        lPFCpy.fill {|i| 3.7 * i}
        raise "Failure to update in-memory state after fill(2)" unless lPF["test_collections__v2"] == lPFCpy
        raise "Failure to update persistent state after fill(2)" unless lPF.refreshPIN()["test_collections__v2"] == lPFCpy
        lPF["test_collections__v2"].fill("zzz", 3)
        lPFCpy.fill("zzz", 3)
        raise "Failure to update in-memory state after fill(3)" unless lPF["test_collections__v2"] == lPFCpy
        raise "Failure to update persistent state after fill(3)" unless lPF.refreshPIN()["test_collections__v2"] == lPFCpy
        lPF["test_collections__v2"].fill(3) {|i| "sss" * i}
        lPFCpy.fill(3) {|i| "sss" * i}
        raise "Failure to update in-memory state after fill(4)" unless lPF["test_collections__v2"] == lPFCpy
        raise "Failure to update persistent state after fill(4)" unless lPF.refreshPIN()["test_collections__v2"] == lPFCpy
        lPF["test_collections__v2"].fill(4, 2) {|i| "aa" * i}
        lPFCpy.fill(4, 2) {|i| "aa" * i}
        raise "Failure to update in-memory state after fill(5)" unless lPF["test_collections__v2"] == lPFCpy
        raise "Failure to update persistent state after fill(5)" unless lPF.refreshPIN()["test_collections__v2"] == lPFCpy
        lPF["test_collections__v2"].fill(5..7) {|i| "b" * i}
        lPFCpy.fill(5..7) {|i| "b" * i}
        raise "Failure to update in-memory state after fill(6)" unless lPF["test_collections__v2"] == lPFCpy
        raise "Failure to update persistent state after fill(6)" unless lPF.refreshPIN()["test_collections__v2"] == lPFCpy

        puts "Performing unshifts."
        lPF["test_collections__v2"].unshift("3.1415")
        lPFCpy.unshift("3.1415")
        raise "Failure to update in-memory state after unshift(1)" unless lPF["test_collections__v2"] == lPFCpy
        raise "Failure to update persistent state after unshift(1)" unless lPF.refreshPIN()["test_collections__v2"] == lPFCpy
        lPF["test_collections__v2"].unshift("bla", "bli", "ble")
        lPFCpy.unshift("bla", "bli", "ble")
        raise "Failure to update in-memory state after unshift(2)" unless lPF["test_collections__v2"] == lPFCpy
        raise "Failure to update persistent state after unshift(2)" unless lPF.refreshPIN()["test_collections__v2"] == lPFCpy

        puts "Performing sorts/shuffles/etc."
        lPS = lNewTest.call("test_collections__v3", 0.upto(50).map{|j| rand()})
        lPSCpy = Array.new(lPS["test_collections__v3"])
        lPS["test_collections__v3"].sort!
        lPSCpy.sort!
        raise "Failure to update in-memory state after sort!(1)" unless lPS["test_collections__v3"] == lPSCpy
        raise "Failure to update persistent state after sort!(1)" unless lPS.refreshPIN()["test_collections__v3"] == lPSCpy
        lPS["test_collections__v3"].sort! {|x,y| y <=> x}
        lPSCpy.sort! {|x,y| y <=> x}
        raise "Failure to update in-memory state after sort!(2)" unless lPS["test_collections__v3"] == lPSCpy
        raise "Failure to update persistent state after sort!(2)" unless lPS.refreshPIN()["test_collections__v3"] == lPSCpy
        srand(123); lPS["test_collections__v3"].shuffle!
        srand(123); lPSCpy.shuffle!
        raise "Failure to update in-memory state after shuffle!" unless lPS["test_collections__v3"] == lPSCpy
        raise "Failure to update persistent state after shuffle!" unless lPS.refreshPIN()["test_collections__v3"] == lPSCpy
        lPS["test_collections__v3"].reverse!
        lPSCpy.reverse!
        raise "Failure to update in-memory state after reverse!" unless lPS["test_collections__v3"] == lPSCpy
        raise "Failure to update persistent state after reverse!" unless lPS.refreshPIN()["test_collections__v3"] == lPSCpy
        lPS["test_collections__v3"].collect! {|x| "**#{x}**"}
        lPSCpy.collect! {|x| "**#{x}**"}
        raise "Failure to update in-memory state after collect!" unless lPS["test_collections__v3"] == lPSCpy
        raise "Failure to update persistent state after collect!" unless lPS.refreshPIN()["test_collections__v3"] == lPSCpy
        lPS["test_collections__v3"].map! {|x| x.to_s.gsub(/\*\*/, "==")}
        lPSCpy.map! {|x| x.to_s.gsub(/\*\*/, "==")}
        raise "Failure to update in-memory state after map!" unless lPS["test_collections__v3"] == lPSCpy
        raise "Failure to update persistent state after map!" unless lPS.refreshPIN()["test_collections__v3"] == lPSCpy

        puts "Done."
      }
    },
    #
    # Simple tests for transactions.
    # There are 2 methods for controlling transactions:
    #   1. via pathSQL statements; this requires a keep-alive connection
    #   2. via the connection's startTx/commitTx/rollbackTx public methods, in protobuf mode
    #
    {
      :name => "test_tx1",
      :comment => "Basic assessment of protobuf transactions.",
      :func => lambda\
      {
        # Note: This test is more about the internal streaming mechanics than about anything meaningful tx-wise.
        lPID = lAffinity.q("INSERT (txtest) VALUES (5);")[0]["id"]
        lAffinity.q("INSERT (txtest) VALUES (125);")
        lAffinity.q("SELECT * WHERE (txtest > 100);")
        lPINs = Affinity::PIN.loadPINs(lAffinity.qProto("SELECT * FROM @#{lPID};"))
        puts "found #{lPINs.length} results."
        lAffinity.startTx("main");
        lAffinity.startTx("property changes");
        lPINs[0].store("txtest", 6);
        lPINs[0].store("someotherprop", 6);
        lAffinity.commitTx
        lChk = Affinity::PIN.loadPINs(lAffinity.qProto("SELECT * WHERE txtest > 100;"))
        puts "newselect: #{lChk.inspect}"
        lCondPIN = if lChk.length > 0 and !lChk[0].pid.nil? then lChk[0] else nil end
        lCondPIN.store("conditionalprop", 6) unless lCondPIN.nil?
        lAffinity.commitTx
        puts "Done."
      }
    },
    {
      :name => "test_tx2",
      :comment => "Basic assessment of protobuf transactions.",
      :func => lambda\
      {
        # Note: This test is more about the internal streaming mechanics than about anything meaningful tx-wise.
        # Note: Unlike in test_tx1, here the results returned by Affinity are mixed (prop sets + select)...
        lPID = lAffinity.q("INSERT (txtest) VALUES (5);")[0]["id"]
        lAffinity.q("INSERT (txtest) VALUES (125);")
        lAffinity.q("SELECT * WHERE (txtest > 100);")
        lPINs = Affinity::PIN.loadPINs(lAffinity.qProto("SELECT * FROM @#{lPID}"))
        puts "found #{lPINs.length} results."
        lAffinity.startTx("main");
        lPINs[0].store("txtest", 6);
        lPINs[0].store("someotherprop", 6);
        lChk = Affinity::PIN.loadPINs(lAffinity.qProto("SELECT * WHERE (txtest > 100);"))
        puts "newselect: #{lChk.inspect}"
        lCondPIN = if lChk.length > 0 and !lChk[0].pid.nil? then lChk[0] else nil end
        lCondPIN.store("conditionalprop", 6) unless lCondPIN.nil?
        lAffinity.commitTx
        puts "Done."
      }
    }, 
    {
      :name => "test_tx_simple_write",
      :comment => "Basic assessment of protobuf transactions.",
      :func => lambda\
      {
        lAffinity.startTx
        puts "newpin: #{lAffinity.createPINs([Affinity::PIN[{:test_tx_simple_write=>"kept1"}]]).inspect}"
        lAffinity.commitTx

        # bugzilla #307...
        #lAffinity.startTx
        #puts "newpin: #{lAffinity.createPINs([Affinity::PIN[{:test_tx_simple_write=>"dropped"}]]).inspect}"
        #lAffinity.rollbackTx

        lAffinity.startTx
        puts "newpin: #{lAffinity.createPINs([Affinity::PIN[{:test_tx_simple_write=>"kept2"}]]).inspect}"
        lAffinity.commitTx

        lAffinity.startTx
        lCnt = 0
        Affinity::PIN.loadPINs(lAffinity.qProto("SELECT WHERE EXISTS(test_tx_simple_write);")).each do |pin|
          puts "result: #{pin.inspect}"
          raise "Failure" if pin["test_tx_simple_write"] == "dropped"
          lCnt += 1
        end
        lAffinity.commitTx
        raise "Failure" unless lCnt > 0
        puts "Done."
      }
    },
    {
      :name => "test_tx_relying_on_keep_alive",
      :comment => "Transactions in pure pathSQL (relies on keep-alive).",
      :func => lambda\
      {
        if !lAffinity.keptAlive()
          puts "warning: this test requires a keep-alive connection - skipped."
          return
        end
        puts "Creating an object and committing."
        lAffinity.q "START TRANSACTION;"
        lPID = lAffinity.q("INSERT (tx_keepalive_committed) VALUES (1);")[0]["id"]
        lAffinity.q "COMMIT;"
        puts "Adding a property and rolling back."
        lAffinity.q "START TRANSACTION;"
        lAffinity.q "UPDATE @#{lPID} SET tx_keepalive_rolledback=2;"
        lRes1 = lAffinity.q "SELECT * FROM @#{lPID};"
        raise "Failure before rollback" unless lRes1[0].has_key? "tx_keepalive_committed" and lRes1[0].has_key? "tx_keepalive_rolledback"
        lAffinity.q "ROLLBACK;"
        lRes2 = lAffinity.q "SELECT * FROM @#{lPID};"
        raise "Failure after rollback" unless lRes2[0].has_key? "tx_keepalive_committed" and !lRes2[0].has_key? "tx_keepalive_rolledback"
        puts "Done."
      }
    },
    #
    # Other unit tests.
    #
    {
      :name => "test_types",
      :comment => "Native vs Affinity basic types.", # Note: Very similar to testtypes1.py (in python)...
      :func => lambda\
      {
        lCheck = lambda\
        {
          |pin, value, vt|
          raise "Failure on value1" unless value == pin['http://localhost/afy/property/testtypes1/value1']
          raise "Failure on value2" unless value == pin['http://localhost/afy/property/testtypes1/value2']
          raise "Failure on value1's type" unless vt == pin.extras['http://localhost/afy/property/testtypes1/value1'][0].vtype
          raise "Failure on value2's type" unless vt == pin.extras['http://localhost/afy/property/testtypes1/value2'][0].vtype
        }

        # VT_STRING
        lValue = "Hello how are you";
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES ('#{lValue}');"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_STRING)

        # VT_BSTR
        lValue = Affinity::PIN::ByteArray.new("Hello how are you")
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (X'#{lValue.bytes.map {|c| "%02x" % c}.compact.join("")}');"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_BSTR)

        # VT_URL
        lValue = Affinity::PIN::Url.new("urn:issn:1234-5678")
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (U'#{lValue}');"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_URL)

        # VT_INT
        lValue = 12345
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lValue});"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_INT)

        # VT_UINT
        lValue = 12345
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lValue}u);"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = [lValue, Affinity::PIN::Extra.new(nil, AffinityPB::Value::ValueType::VT_UINT)]
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_UINT)

        # VT_INT64
        lValue = -8589934592
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lValue});"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_INT64)

        # VT_UINT64
        lValue = 8589934592
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lValue}U);"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = [lValue, Affinity::PIN::Extra.new(nil, AffinityPB::Value::ValueType::VT_UINT64)]
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_UINT64)

        # VT_FLOAT
        lValue = 123.5
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lValue}f);"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = [lValue, Affinity::PIN::Extra.new(nil, AffinityPB::Value::ValueType::VT_FLOAT)]
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_FLOAT)

        # VT_DOUBLE
        lValue = 123.5
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lValue});"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_DOUBLE)

        # VT_BOOL
        lValue = true
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lValue});"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_BOOL)

        # VT_DATETIME
        lValue = Time.now.utc
        puts "date: #{lValue}"
        lValueStr = lValue.strftime("%4Y-%2m-%2d %2H:%2M:%2S")
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (TIMESTAMP'#{lValueStr}');"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        raise "Failure on value1" unless lValue.to_s == lPin['http://localhost/afy/property/testtypes1/value1'].utc.to_s
        raise "Failure on value2" unless lValue.to_s == lPin['http://localhost/afy/property/testtypes1/value2'].utc.to_s
        raise "Failure on value1's type" unless AffinityPB::Value::ValueType::VT_DATETIME == lPin.extras['http://localhost/afy/property/testtypes1/value1'][0].vtype
        raise "Failure on value2's type" unless AffinityPB::Value::ValueType::VT_DATETIME == lPin.extras['http://localhost/afy/property/testtypes1/value2'][0].vtype
        lAffinity.qProto("UPDATE #{lPin.pid} ADD \"http://localhost/afy/property/testtypes1/value1\"=123;")
        lPin.refreshPIN
        lReferenced1 = lPin

        # VT_REFID
        lValue = Affinity::PIN::Ref.fromPID(lReferenced1.pid)
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lReferenced1.pid});"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_REFID)

        # VT_REFIDPROP
        lValue = Affinity::PIN::Ref.new(lReferenced1.pid.localPID, lReferenced1.pid.ident, 'http://localhost/afy/property/testtypes1/value1')
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lReferenced1.pid}.\"http://localhost/afy/property/testtypes1/value1\");"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_REFIDPROP)

        # VT_REFIDELT
        lValue = Affinity::PIN::Ref.new(lReferenced1.pid.localPID, lReferenced1.pid.ident, 'http://localhost/afy/property/testtypes1/value1', lReferenced1.extras['http://localhost/afy/property/testtypes1/value1'][1].eid)
        lPin = Affinity::PIN.loadPINs(lAffinity.qProto("INSERT (\"http://localhost/afy/property/testtypes1/value1\") VALUES (#{lReferenced1.pid}.\"http://localhost/afy/property/testtypes1/value1\"[#{lValue.eid}]);"))[0]
        lPin['http://localhost/afy/property/testtypes1/value2'] = lValue
        lPin.refreshPIN
        lCheck.call(lPin, lValue, AffinityPB::Value::ValueType::VT_REFIDELT)

        # TODO: VT_INTERVAL
        # TODO: VT_EXPR
        # TODO: VT_QUERY
        # TODO: VT_CURRENT - available?
        # TODO: VT_ENUM - available?
        # TODO: VT_DECIMAL - available?
        # TODO: VT_URIID - purpose? vs VT_REFID?
        # TODO: VT_IDENTITY - purpose?
        # TODO: VT_REFCID - purpose?
        # TODO: VT_RANGE - purpose?
        # NOTE: VT_ARRAY is tested with collections.

        puts "Done."
      }
    },
    {
      :name => "test_qnames",
      :comment => "Quick demonstration of qnames (relies on keep-alive).",
      :func => lambda\
      {
        if !lAffinity.keptAlive()
          puts "warning: this test requires a keep-alive connection - skipped."
          return
        end
        lAffinity.q "SET PREFIX myqnamec: 'http://localhost/afy/class/test_qnames/';"
        lAffinity.q "SET PREFIX myqnamep: 'http://localhost/afy/property/test_qnames/';"
        if 0 == lAffinity.qCount("SELECT * FROM afy:ClassOfClasses WHERE BEGINS(afy:classID, 'http://localhost/afy/class/test_qnames/');")
          lAffinity.q "CREATE CLASS myqnamec:pos AS SELECT * WHERE EXISTS(myqnamep:x) AND EXISTS(myqnamep:y);"
        end
        lAffinity.q "INSERT (myqnamep:x, myqnamep:y) VALUES (#{rand()}, #{rand()});"
        lRes = Affinity::PIN.loadPINs(lAffinity.qProto "SELECT * FROM myqnamec:pos;")
        puts "result: #{lRes.inspect}"
        puts "Done."
      }
    },
    #
    # Simple pseudo-application: personal photo server storage back-end.
    #
    # This example relies exclusively on the pathSQL mode with json responses (no protobuf).
    # The transaction-control statements are disabled if the connection is not using keep-alive.
    # The data model is essentially relational (no collection or reference, just classes).
    #
    # This application simulates a personal photo server, containing records for actual photos,
    # along with a registry of guests, each being granted individual and group privileges to
    # see some of the photos. The application does the following:
    #
    #   1. it declares a few classes (if not already present, from previous runs)
    #   2. it removes any old data from previous runs
    #   3. it creates records for fake "photos" (it walks the kernel tests directory and interprets any cpp file as if it were a photo file)
    #   4. it creates a bunch of users and groups
    #   5. it grants access to photos, to specific users and groups (randomly)
    #   6. it counts how many photos a specific guest can view
    #   7. it double-checks everything with an in-memory representation
    #
    {
      :name => "test_app_photos1",
      :comment => "Simple app, using pathSQL+JSON only.", # Note: Very similar to testphotos1.py (in python)...
      :func => lambda\
      {
        puts "warning: transactions in this test will not be perfectly respected without a keep-alive connection." if (!lAffinity.keptAlive())

        # Misc helpers.
        lChkCount = lambda {|name, expected, actual| puts "#{(expected == actual ? "YEAH" : "\n***\n*** WARNING")}: expected #{expected} #{name}, found #{actual}."; raise "Failure" unless expected == actual; }
        lChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        lRandomString = lambda {|len| (0..len).map{lChars[rand(lChars.length)].chr}.join }
        lStartTx = lambda { if lAffinity.keptAlive() then lAffinity.q("START TRANSACTION;") end }
        lCommitTx = lambda { if lAffinity.keptAlive() then lAffinity.q("COMMIT;") end }

        # Return an array of all files with extension ext under dir (recursively).
        lWalkDir = lambda\
        {
          |dir, ext|
          result = []
          Find.find(dir) do |f|
            result << {:filename=>f, :dirname=>dir} if f.match(/\.#{ext}$/)
          end
          result
        }

        # Perform a side-by-side in-memory comparison, to validate every step of the test.
        class InMemoryChk
          def initialize()
            @photos = {}
            @users = {}
            @groups = {}
          end
          def tagPhoto(photo, tag) if !@photos.has_key? photo then @photos[photo] = {} end; @photos[photo][tag] = 1; end
          def setUserGroup(user, group) if !@users.has_key? user then @users[user] = {:tags=>{}} end; if !@groups.has_key? group then @groups[group] = {} end; @users[user][:group] = group; end
          def addUserPrivilege(user, tag) if !@users.has_key? user then @users[user] = {:tags=>{}} end; @users[user][:tags][tag] = 1; end
          def addGroupPrivilege(group, tag) if !@groups.has_key? group then @groups[group] = {} end; @groups[group][tag] = 1; end
          def getTags_usPriv(user) if !@users.has_key? user then [] else @users[user][:tags].keys end; end
          def getTags_grPriv(user) if !@users.has_key? user then [] else @groups[@users[user][:group]].keys end; end
          def getUserTags(user) _tags = {}; getTags_usPriv(user).each do |iT| _tags[iT] = 1 end; getTags_grPriv(user).each do |iT| _tags[iT] = 1 end; _tags.keys(); end
          def countPhotos(user) if !@users.has_key? user then return 0 end; _tags = getUserTags(user); _photos = {}; @photos.each_key do |iP| @photos[iP].each_key do |iT| if !_tags.index(iT).nil? then _photos[iP] = 1 end; end; end; _photos.keys.length; end
        end
        lInMemoryChk = InMemoryChk.new();

        # Create the specified photo object in the db.
        lCreatePhoto = lambda\
        {
          |dir, fn|
          fullPath = "#{dir}/#{fn}";
          puts "adding file #{fullPath}"
          fctime = File.stat(fullPath).ctime
          fdate = fctime.strftime("%4Y-%2m-%2d")
          ftime = fctime.strftime("%2H:%2M:%2S")
          fhash = `uuidgen`.strip.gsub(/-/, "")
          lAffinity.q("INSERT \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\"='#{fhash}', \"http://www.w3.org/2001/XMLSchema#date\"=TIMESTAMP'#{fdate}', \"http://www.w3.org/2001/XMLSchema#time\"=INTERVAL'#{ftime}', \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileUrl\"='#{dir}', \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileName\"='#{fn}';")
        }

        # Create the specified user object in the db.
        lCreateUser = lambda\
        {
          |user, group|
          lInMemoryChk.setUserGroup(user, group)
          lAffinity.q("INSERT \"http://xmlns.com/foaf/0.1/mbox\"='#{user}', \"http://www.w3.org/2002/01/p3prdfv1#user.login.password\"='#{lRandomString.call(20)}', \"http://xmlns.com/foaf/0.1/member/adomain:Group\"='#{group}';")
          puts "group #{group} contains #{lAffinity.qCount("SELECT * FROM \"http://localhost/afy/class/testphotos1/user\" WHERE \"http://xmlns.com/foaf/0.1/member/adomain:Group\"='" + group + "';")} users"
        }

        # Select all distinct group names.
        lSelectDistinctGroups = lambda\
        {
          # Review: eventually Affinity will allow to SELECT DISTINCT(groupid) FROM users...
          groupIds = {}
          lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/user\";").each do |iP|
            groupIds[iP["http://xmlns.com/foaf/0.1/member/adomain:Group"]] = 1
          end
          groupIds.keys
        }

        # Create the specified tag and assign it to a random selection of photos.
        lAssignTagRandomly = lambda\
        {
          |tagname|
          tagPhoto = lambda\
          {
            |photo|
            return if rand() > 0.10
            lInMemoryChk.tagPhoto(photo["http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash"], tagname)
            lAffinity.q("INSERT \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\"='#{photo["http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash"]}', \"http://code.google.com/p/tagont/hasTagLabel\"='#{tagname}';")
          }
          lStartTx.call()
          tagCount = lAffinity.qCount("SELECT * FROM \"http://localhost/afy/class/testphotos1/tag\" WHERE \"http://code.google.com/p/tagont/hasTagLabel\"='#{tagname}';")
          if 0 == tagCount then puts "adding tag #{tagname}"; lAffinity.q("INSERT \"http://code.google.com/p/tagont/hasTagLabel\"='#{tagname}';") end
          lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/photo\";").each do |photo|
            tagPhoto.call(photo)
          end
          lCommitTx.call()
        }

        # Assign a random selection of tags to each existing group.
        lAssignGroupPrivilegesRandomly = lambda\
        {
          groupIds = lSelectDistinctGroups.call
          puts "groups: #{groupIds.inspect}"
          tags = lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/tag\";")
          puts "tags: #{tags.map{|t| t["http://code.google.com/p/tagont/hasTagLabel"]}.compact.join(" ")}"
          lOneTag = lambda\
          {
            |groupId, tagName|
            lInMemoryChk.addGroupPrivilege(groupId, tagName)
            lAffinity.q("INSERT \"http://code.google.com/p/tagont/hasTagLabel\"='#{tagName}', \"http://code.google.com/p/tagont/hasVisibility\"='#{groupId}';")
          }
          lOneIter = lambda\
          {
            |groupId|
            tags.each do |iT|
              next if rand() < 0.5
              lOneTag.call(groupId, iT["http://code.google.com/p/tagont/hasTagLabel"])
            end
          }
          groupIds.each do |gid| lOneIter.call(gid) end
        }

        # Assign a random selection of tags to each existing user.
        lAssignUserPrivilegesRandomly = lambda\
        {
          users = lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/user\";")
          puts "users: #{users.map{|u| u["http://xmlns.com/foaf/0.1/mbox"]}.compact.join(" ")}"
          tags = lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/tag\";")
          puts "tags: #{tags.map{|t| t["http://code.google.com/p/tagont/hasTagLabel"]}.compact.join(" ")}"
          lOneTag = lambda\
          {
            |userName, tagName|
            lInMemoryChk.addUserPrivilege(userName, tagName)
            lAffinity.q("INSERT \"http://code.google.com/p/tagont/hasTagLabel\"='#{tagName}', \"http://code.google.com/p/tagont/hasVisibility\"='#{userName}';")
          }
          lOneIter = lambda\
          {
            |user|
            randRange = rand()
            tags.each do |iT|
              next if rand() < randRange
              lOneTag.call(user["http://xmlns.com/foaf/0.1/mbox"], iT["http://code.google.com/p/tagont/hasTagLabel"])
            end
          }
          users.each do |u| lOneIter.call(u) end
        }

        # Find users who can see the first 5 of the specified tags.
        lGetUsersOfInterest = lambda\
        {
          |tags|
          firstTags = tags.map{ |t| "'#{ t["http://code.google.com/p/tagont/hasTagLabel"] }'" }.compact()[0, 5]
          result = []
          lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/privilege\"(#{firstTags.join(",")}) AS p JOIN \"http://localhost/afy/class/testphotos1/user\" AS u ON (p.\"http://code.google.com/p/tagont/hasVisibility\" = u.\"http://xmlns.com/foaf/0.1/mbox\");").each do |priv|
            result << priv[0]["http://code.google.com/p/tagont/hasVisibility"]
          end
          puts "users that have one of #{firstTags.inspect}: #{result.join(",")}"
          result
        }

        # Count how many photos the specified user can see.
        lCountUserPhotos = lambda\
        {
          |userName|
          tags = {} # Accumulate user and group privileges.
          # Check user privileges.
          usPriv_exp = lInMemoryChk.getTags_usPriv(userName).sort
          lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/privilege\" WHERE \"http://code.google.com/p/tagont/hasVisibility\"='#{userName}';").each do |priv|
            tags[priv["http://code.google.com/p/tagont/hasTagLabel"]] = 1
          end
          usPriv_act = tags.keys.sort
          puts "user #{userName} has user privilege tags #{usPriv_act.join(",")}"
          puts "WARNING: expected user-privilege tags #{usPriv_exp.join(",")}" unless usPriv_act == usPriv_exp
          # Check group privileges.
          tags_exp = lInMemoryChk.getUserTags(userName).sort
          lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/privilege\" AS p JOIN \"http://localhost/afy/class/testphotos1/user\"('#{userName}') AS u ON (p.\"http://code.google.com/p/tagont/hasVisibility\" = u.\"http://xmlns.com/foaf/0.1/member/adomain:Group\");").each do |priv|
            tags[priv[0]["http://code.google.com/p/tagont/hasTagLabel"]] = 1;
          end
          tags_act = tags.keys.sort
          puts "user #{userName} has tags #{tags_act.join(",")}"
          puts "WARNING: expected tags #{tags_exp.join(",")}" unless tags_act == tags_exp
          # Count unique photos.
          uniquePhotos = {}
          quotedTags = tags.keys.map{ |t| "'#{t}'" }.compact
          lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/photo\" AS p JOIN \"http://localhost/afy/class/testphotos1/tagging\" AS t ON (p.\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\" = t.\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\") WHERE t.\"http://code.google.com/p/tagont/hasTagLabel\" IN (#{quotedTags.join(",")});").each do |photo|
            uniquePhotos[photo[0]["http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash"]] = 1
          end
          cnt = uniquePhotos.keys.length
          lChkCount.call("photos that can be viewed by #{userName}", lInMemoryChk.countPhotos(userName), cnt)
          cnt
        }

        # Run the scenario.
        if 0 == lAffinity.qCount("SELECT * FROM afy:ClassOfClasses WHERE BEGINS(afy:classID, 'http://localhost/afy/class/testphotos1/');")
          puts "Creating classes."
          lAffinity.q("CREATE CLASS \"http://localhost/afy/class/testphotos1/photo\" AS SELECT * WHERE \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\" IN :0 AND EXISTS(\"http://www.w3.org/2001/XMLSchema#date\") AND EXISTS(\"http://www.w3.org/2001/XMLSchema#time\") AND EXISTS(\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileUrl\") AND EXISTS (\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileName\");")
          lAffinity.q("CREATE CLASS \"http://localhost/afy/class/testphotos1/tag\" AS SELECT * WHERE \"http://code.google.com/p/tagont/hasTagLabel\" in :0 AND NOT EXISTS(\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\") AND NOT EXISTS(\"http://code.google.com/p/tagont/hasVisibility\");")
          lAffinity.q("CREATE CLASS \"http://localhost/afy/class/testphotos1/tagging\" AS SELECT * WHERE EXISTS(\"http://code.google.com/p/tagont/hasTagLabel\") AND \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\" in :0;")
          lAffinity.q("CREATE CLASS \"http://localhost/afy/class/testphotos1/user\" AS SELECT * WHERE \"http://xmlns.com/foaf/0.1/mbox\" in :0 AND EXISTS(\"http://www.w3.org/2002/01/p3prdfv1#user.login.password\") AND EXISTS(\"http://xmlns.com/foaf/0.1/member/adomain:Group\");")
          lAffinity.q("CREATE CLASS \"http://localhost/afy/class/testphotos1/privilege\" AS SELECT * WHERE \"http://code.google.com/p/tagont/hasTagLabel\" in :0 AND EXISTS(\"http://code.google.com/p/tagont/hasVisibility\");")
        else
          puts "Deleting old data."
          lAffinity.q("DELETE FROM \"http://localhost/afy/class/testphotos1/photo\";")
          lAffinity.q("DELETE FROM \"http://localhost/afy/class/testphotos1/tag\";")
          lAffinity.q("DELETE FROM \"http://localhost/afy/class/testphotos1/tagging\";")
          lAffinity.q("DELETE FROM \"http://localhost/afy/class/testphotos1/user\";")
          lAffinity.q("DELETE FROM \"http://localhost/afy/class/testphotos1/privilege\";")
        end
        puts "Creating a few photos."
        lStartTx.call()
        lFiles = lWalkDir.call("../tests", "cpp");
        lFiles.each do |f| lCreatePhoto.call(f[:dirname], f[:filename]) end
        lCommitTx.call()
        lChkCount.call("photos", lFiles.length, lAffinity.qCount("SELECT * FROM \"http://localhost/afy/class/testphotos1/photo\";"))
        lSomeTags = ["cousin_vinny", "uncle_buck", "sister_suffragette", "country", "city", "zoo", "mountain_2010", "ocean_2004", "Beijing_1999", "Montreal_2003", "LasVegas_2007", "Fred", "Alice", "sceneries", "artwork"]
        puts "Creating a few tags."
        lStartTx.call()
        lSomeTags.each do |tag| lAssignTagRandomly.call(tag) end
        lCommitTx.call()
        lGroups = ["friends", "family", "public"]
        lUsers = ["ralph@peanut.com", "stephen@banana.com", "wilhelm@orchestra.com", "sita@marvel.com", "anna@karenina.com", "leo@tolstoy.com", "peter@pan.com", "jack@jill.com", "little@big.com", "john@hurray.com", "claire@obscure.com", "stanley@puck.com", "grey@ball.com", "john@wimbledon.com", "mark@up.com", "sabrina@cool.com"]
        puts "Creating a few users and groups."
        lStartTx.call()
        lUsers.each do |user| group = lGroups[rand(lGroups.length)]; lCreateUser.call(user, group); end
        lCommitTx.call()
        lChkCount.call("users", lUsers.length, lAffinity.qCount("SELECT * FROM \"http://localhost/afy/class/testphotos1/user\";"))
        lChkCount.call("groups", lGroups.length, lSelectDistinctGroups.call.length)
        lAssignGroupPrivilegesRandomly.call
        lAssignUserPrivilegesRandomly.call
        puts "#{lAffinity.qCount("SELECT * FROM \"http://localhost/afy/class/testphotos1/privilege\";")} privileges assigned."
        lGetUsersOfInterest.call(lAffinity.q("SELECT * FROM \"http://localhost/afy/class/testphotos1/tag\";")).each do |user|
          lCountUserPhotos.call(user)
        end
        puts "Done."
      }
    },
  ]

  lTests.each do |iTest|
    if /^test/i.match(iTest[:name]) # Allow to comment out a test by just modifying the beginning of its name.
      puts "Running #{iTest[:name]}"
      iTest[:func].call
    end
  end
end

=begin TODO: translate when I have a chance

  /**
   * Simple pseudo-application: graph db benchmark.
   *
   * This example uses a mixture of pure pathSQL (with json responses) and protobuf.
   * Some of the transaction-control statements are disabled if the connection is not using keep-alive.
   * The data model relies heavily on collections and references.
   *
   * This application reads input files describing a social graph of people with friends,
   * each of which owns a project (or directory) structure, with access privileges granted
   * to their friends on a per-sub-project basis. This project structure also contains photos.
   *
   * The phase1 loads the data into the store.
   * The phase2 performs typical graph queries, using path expressions.
   */

  test_benchgraph1_phase1_load:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lTxSize = 1024;

    // Helper function, to process the text-based input files (people_*.txt, projects_*.txt, photos_*.txt).
    // Can function synchronously or asynchronously, depending on the parameters.
    //   _pFN: the file name
    //   _pDoWhat: a function accepting (a line of text, the line index, and an optional callback to pursue the iteration asynchronously); if this function returns false, the iteration is pursued synchronously by the caller.
    //   _pFinalCallback: an optional callback to be invoked at the end of the chain, in the case of asynchronous execution; accepts the total number of lines.
    //   _pMaxLineLen: an optional specification of the maximum expected length for any given line in the text file.
    var lProcessLines = function(_pFN, _pDoWhat, _pFinalCallback, _pMaxLineLen)
    {
      var _lMaxLineLen = _pMaxLineLen || 256;
      var _lTotalLineCount = 0;
      var _lF = lib_fs.openSync(_pFN, "r");
      lib_assert.ok(undefined != _lF);
      var _lPos = 0;
      var _lNextLine =
        function()
        {
          var __lRead = lib_fs.readSync(_lF, _lMaxLineLen, _lPos, "ascii").toString();
          var __lMatch = (undefined != __lRead) ? __lRead.match(/(.*)\n/) : null;
          if (undefined == __lMatch)
          {
            lib_fs.close(_lF);
            if (undefined != _pFinalCallback)
              _pFinalCallback(_lTotalLineCount);
            return _lTotalLineCount;
          }
          var __lLine = __lMatch[1];
          _lPos += __lLine.length + 1;
          _lTotalLineCount++;
          if (undefined == _pDoWhat || !_pDoWhat(__lLine, _lTotalLineCount, _lNextLine))
            return _lNextLine();
        };
      return _lNextLine();
    }

    // Other helpers.
    var lProcessLinesSync = function(_pFN, _pDoWhat, _pMaxLineLen) { return lProcessLines(_pFN, function(__pLine, __pLineNum) { if (undefined != _pDoWhat) { _pDoWhat(__pLine, __pLineNum); } return false; }); }
    var lCountLinesInFile = function(_pFN) { return lProcessLinesSync(_pFN); }
    var lParallelExecHub = function(_pMaxCount, _pCallback)
    {
      var _lCount = 0;
      return {punch:function() { _lCount++; if (_lCount == _pMaxCount) { _pCallback(); } else { lib_assert.ok(_lCount < _pMaxCount); } }};
    }
    var lStartTx = function(_pSS) { if (lAffinity.keptAlive()) { lAffinity.q("START TRANSACTION;", _pSS.next); } else { _pSS.next(); } }
    var lCommitTx = function(_pSS) { if (lAffinity.keptAlive()) { lAffinity.q("COMMIT;", _pSS.next); } else { _pSS.next(); } }
    var lWritePercent = function(_pPercent) { var _lV = _pPercent.toFixed(0); for (var _i = 0; _i < _lV.length + 1; _i++) { process.stdout.write("\b"); } process.stdout.write("" + _pPercent.toFixed(0) + "%"); }

    // Declaration of classes.
    var lClassesExist = false;
    var lOnSelectClasses = function(pError, pResponse) { console.log("substep " + lSS.curstep()); if (pError) console.log("\n*** ERROR: " + pError + "\n"); else { console.log("Result from step " + lSS.curstep() + ":" + JSON.stringify(pResponse)); lClassesExist = (pResponse && pResponse.length > 0); lSS.next(); } }
    lSS.push(function() { console.log("Creating classes."); lAffinity.q("SELECT * FROM afy:ClassOfClasses WHERE BEGINS(afy:classID, 'http://localhost/afy/class/benchgraph1/');", lOnSelectClasses); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lAffinity.q("CREATE CLASS \"http://localhost/afy/class/benchgraph1/orgid\" AS SELECT * WHERE \"http://localhost/afy/property/benchgraph1/orgid\" IN :0;", lSS.simpleOnResponse); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lAffinity.q("CREATE CLASS \"http://localhost/afy/class/benchgraph1/fid\" AS SELECT * WHERE \"http://localhost/afy/property/benchgraph1/fid\" in :0;", lSS.simpleOnResponse); });

    // Definition of input files.
    var lPeopleLineCount = 0, lProjectsLineCount = 0, lPhotosLineCount = 0;
    var lParamsActual = "50_10_50_100";
    var lPeopleFileName = "./tests_data/people_" + lParamsActual + ".txt";
    var lProjectsFileName = "./tests_data/projects_" + lParamsActual + ".txt";
    var lPhotosFileName = "./tests_data/photos_" + lParamsActual + ".txt";
    lSS.push(
      function() 
      {
        lPeopleLineCount = lCountLinesInFile(lPeopleFileName);
        lProjectsLineCount = lCountLinesInFile(lProjectsFileName);
        lPhotosLineCount = lCountLinesInFile(lPhotosFileName);
        lSS.next();
      });

    // First scan of the 'people' file: create all people.
    lSS.push(function() { console.log("Creating " + lPeopleLineCount + " people..."); lStartTx(lSS); });
    lSS.push(
      function()
      {
        // Parallel execution (create all people in parallel)...
        var _lHub = lParallelExecHub(lPeopleLineCount, lSS.next);
        lProcessLinesSync(
          lPeopleFileName,
          function(_pLine, _pLineCount)
          {
            var _lM = _pLine.match(/^\(([0-9]+)\s+\'([A-Za-z\s]+)\'\s+\'([A-Za-z\s]+)\'\s+\'([A-Za-z\s]+)\'\s+\'([A-Za-z\-\s]+)\'\s+\'([A-Za-z\s]+)\'\s+\'([A-Z][0-9][A-Z]\s+[0-9][A-Z][0-9])\'/)
            if (undefined != _lM)
            {
              lAffinity.q(
                "INSERT (\"http://localhost/afy/property/benchgraph1/orgid\", \"http://localhost/afy/property/benchgraph1/firstname\", \"http://localhost/afy/property/benchgraph1/middlename\", \"http://localhost/afy/property/benchgraph1/lastname\", \"http://localhost/afy/property/benchgraph1/occupation\", \"http://localhost/afy/property/benchgraph1/country\", \"http://localhost/afy/property/benchgraph1/postalcode\") VALUES (" +
                _lM[1] + ", '" +
                _lM[2] + "', '" + _lM[3] + "', '" + _lM[4] + "', '" +
                _lM[5] + "', '" + _lM[6] + "', '" + _lM[7] + "');", _lHub.punch());
            }
            else console.log("WARNING: Couldn't match attributes on " + _pLine);
          });
      });
    lSS.push(function() { lCommitTx(lSS); });
    lSS.push(function() { console.log("Created " + lPeopleLineCount + " people."); lSS.next(); });

    // Second scan of the 'people' file: create all relationships.
    var lRelCount = 0;
    lSS.push(function() { console.log("Creating relationships..."); lStartTx(lSS); });
    lSS.push(
      function()
      {
        // Serial execution (create relationships one person at a time)...
        process.stdout.write("  0%");
        lProcessLines(
          lPeopleFileName,
          function(_pLine, _pLineCount, _pNextLine)
          {
            var __lM = _pLine.match(/^\(([0-9]+).*\(([0-9\s]*)\)\)$/)
            if (undefined != __lM)
            {
              // Parallel execution (create all of a person's relationships in parallel)...
              var __lRefs = __lM[2].split(" ");
              if (0 == __lRefs.length) { _pNextLine(); return; }
              var __lPID1;
              var __lSS = new InstrSeq();
              var __lHub = lParallelExecHub(__lRefs.length, __lSS.next);
              __lSS.push(function() { lAffinity.q("SELECT * FROM \"http://localhost/afy/class/benchgraph1/orgid\"(" + __lM[1] + ");", function(__pE, __pR) { __lPID1 = __pR[0].id; __lSS.next() }); });
              __lSS.push(
                function()
                {
                  for (var ___iR = 0; ___iR < __lRefs.length; ___iR++)
                  {
                    var ___lIter =
                      function()
                      {
                        var ___lPID2;
                        var ___lSS = new InstrSeq();
                        ___lSS.push(function() { lAffinity.q("SELECT * FROM \"http://localhost/afy/class/benchgraph1/orgid\"(" + __lRefs[___iR] + ");", function(___pE, ___pR) { ___lPID2 = ___pR[0].id; ___lSS.next() }); });
                        ___lSS.push(function() { lRelCount++; lAffinity.q("UPDATE @" + __lPID1.toString(16) + " ADD \"http://localhost/afy/property/benchgraph1/friendof\"=@" + ___lPID2.toString(16) + ";", __lHub.punch); });
                        ___lSS.start();
                      };
                    ___lIter();
                  }
                });
              /* Note: This could be enabled once task #223 is resolved.
              __lSS.push(
                function()
                {
                  for (var ___iR = 0; ___iR < __lRefs.length; ___iR++)
                  {
                    lAffinity.q("UPDATE @" + __lPID1.toString(16) + " ADD \"http://localhost/afy/property/benchgraph1/friendof\"=(SELECT * FROM \"http://localhost/afy/class/benchgraph1/orgid\"(" + __lRefs[___iR] + "));", __lHub.punch());
                    lRelCount++;
                  }
                });
              */
              __lSS.push(function() { lWritePercent(100.0 * _pLineCount / lPeopleLineCount); __lSS.next(); });
              __lSS.push(_pNextLine);
              __lSS.start();
            }
            else console.log("WARNING: Couldn't match attributes on " + _pLine);
            return true;
          },
          lSS.next);
      });
    lSS.push(function() { lCommitTx(lSS); });
    lSS.push(function() { console.log(" Created " + lRelCount + " relationships."); lSS.next(); });

    // Create the project (aka directory) structure.
    lSS.push(function() { console.log("Creating " + lProjectsLineCount + " projects..."); lStartTx(lSS); });
    lSS.push(
      function()
      {
        // Serial execution (create one project at a time)...
        process.stdout.write("  0%");
        lProcessLines(
          lProjectsFileName,
          function(_pLine, _pLineCount, _pNextLine)
          {
            var _lM = _pLine.match(/^\(([0-9]+)\s+\'([A-Za-z]+)\'\s+([0-9]+)\s+([0-9]+)\)$/)
            if (undefined != _lM)
            {
              var __lSS = new InstrSeq();
              var __lPIDNewProject;
              __lSS.push(
                function()
                {
                  // Create the new project.
                  lAffinity.q(
                    "INSERT (\"http://localhost/afy/property/benchgraph1/fid\", \"http://localhost/afy/property/benchgraph1/fname\", \"http://localhost/afy/property/benchgraph1/access\") VALUES (" +
                    _lM[1] + ", '" +
                    _lM[2] + "', " +
                    _lM[4] + ");", function(__pE, __pR) { __lPIDNewProject = __pR[0].id; __lSS.next() });
                });
              if ("radix" == _lM[2])
              {
                // If the new project is a root project, retrieve its root owner and link to it.
                var __lPIDOwner;
                __lSS.push(function() { lAffinity.q("SELECT * FROM \"http://localhost/afy/class/benchgraph1/orgid\"(" + _lM[4] + ");", function(___pE, ___pR) { __lPIDOwner = ___pR[0].id; __lSS.next() }); });
                __lSS.push(
                  function()
                  {
                    lAffinity.q(
                      "UPDATE @" + __lPIDOwner.toString(16) + " SET \"http://localhost/afy/property/benchgraph1/rootproject\"=@" + __lPIDNewProject.toString(16) + ";", __lSS.next);
                  });
              }
              else
              {
                // If the new project is not a root project, link it to its parent.
                var __lPIDParentProject;
                __lSS.push(function() { lAffinity.q("SELECT * FROM \"http://localhost/afy/class/benchgraph1/fid\"(" + _lM[3] + ");", function(___pE, ___pR) { __lPIDParentProject = ___pR[0].id; __lSS.next() }); });
                __lSS.push(
                  function()
                  {
                    lAffinity.q(
                      "UPDATE @" + __lPIDParentProject.toString(16) + " ADD \"http://localhost/afy/property/benchgraph1/children\"=@" + __lPIDNewProject.toString(16) + ";", __lSS.next);
                  });
              }
              __lSS.push(function() { lWritePercent(100.0 * _pLineCount / lProjectsLineCount); __lSS.next(); });
              __lSS.push(_pNextLine);
              __lSS.start();
            }
            else console.log("WARNING: Couldn't match attributes on " + _pLine);
            return true;
          },
          lSS.next);
      });
    lSS.push(function() { lCommitTx(lSS); });
    lSS.push(function() { console.log(" Created " + lProjectsLineCount + " projects."); lSS.next(); });

    // Insert the photos in the project structure.
    lSS.push(function() { console.log("Creating " + lPhotosLineCount + " photos..."); lStartTx(lSS); });
    lSS.push(
      function()
      {
        // Serial execution (create one photo at a time)...
        process.stdout.write("  0%");
        lProcessLines(
          lPhotosFileName,
          function(_pLine, _pLineCount, _pNextLine)
          {
            var _lM = _pLine.match(/^\(([0-9]+)\s+\'([A-Za-z0-9\s]+)\'\s+([0-9]+)\)$/)
            if (undefined != _lM)
            {
              var __lSS = new InstrSeq();
              var __lPIDNewPhoto, __lPIDParent;
              __lSS.push(
                function()
                {
                  // Create the new photo.
                  lAffinity.q(
                    "INSERT (\"http://localhost/afy/property/benchgraph1/fid\", \"http://localhost/afy/property/benchgraph1/pname\") VALUES (" +
                    _lM[1] + ", '" +
                    _lM[2] + "');", function(__pE, __pR) { __lPIDNewPhoto = __pR[0].id; __lSS.next() });
                });
              __lSS.push(function() { lAffinity.q("SELECT * FROM \"http://localhost/afy/class/benchgraph1/fid\"(" + _lM[3] + ");", function(___pE, ___pR) { __lPIDParent = ___pR[0].id; __lSS.next() }); });
              __lSS.push(
                function()
                {
                  lAffinity.q(
                    "UPDATE @" + __lPIDParent.toString(16) + " ADD \"http://localhost/afy/property/benchgraph1/children\"=@" + __lPIDNewPhoto.toString(16) + ";", __lSS.next);
                });
              __lSS.push(function() { lWritePercent(100.0 * _pLineCount / lPhotosLineCount); __lSS.next(); });
              __lSS.push(_pNextLine);
              __lSS.start();
            }
            else console.log("WARNING: Couldn't match attributes on " + _pLine);
            return true;
          },
          lSS.next);
      });
    lSS.push(function() { lCommitTx(lSS); });
    lSS.push(function() { console.log(" Created " + lPhotosLineCount + " photos."); lSS.next(); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_benchgraph1_phase2_queries:function(pOnSuccess)
  {
    var lSS = new InstrSeq();

    // Use a sample of ~20 people in the middle of the range.
    var lNumPeople = 0;
    var lPeopleSample = new Array();
    lSS.push(
      function()
      {
        lAffinity.qProto(
          "SELECT * FROM \"http://localhost/afy/class/benchgraph1/orgid\";",
          function(_pE, _pR)
          {
            lNumPeople = _pR.length;
            for (var _iP = 10; _iP < Math.min(_pR.length - 10, 30); _iP++)
              lPeopleSample.push(_pR[_iP]);
            lSS.next();
          })
      });

    // Helper for breadth-first search, with full trace of the solution.
    function BFSearchCtx(pStartOrgid, pEndOrgid, pCallback)
    {
      var _mStartOrgid = pStartOrgid; // The starting value of http://localhost/afy/property/benchgraph1/orgid.
      var _mEndOrgid = pEndOrgid; // The target value of http://localhost/afy/property/benchgraph1/orgid.
      var _mFinalCallback = pCallback; // The final callback to invoke when a solution is found or when no solution can be found.
      var _mBwHops = {}; // A dictionary of backword hops (how did we reach B? from A).
      var _mNextLevel = []; // An array of PINs to visit next time we recurse one level deeper.
      var _mFound = false; // Whether a solution was found.
      var _lGetSolution = function(_pLastOrgid)
      {
        var __lHops = new Array();
        var __iFrom = _pLastOrgid;
        while (__iFrom != _mStartOrgid)
        {
          __lHops.push(__iFrom);
          __iFrom = _mBwHops[__iFrom];
        }
        return __lHops;
      }
      var _lRecurseOne = function(_pSS, _pPIN, _pLevel)
      {
        _pSS.push(
          function()
          {
            if (_mFound) { return; }
            lAffinity.qProto(
              "SELECT * FROM @" + _pPIN.pid.toString(16) + ".\"http://localhost/afy/property/benchgraph1/friendof\";",
              function(__pE, __pR)
              {
                var __lFromOrgid = _pPIN.get("http://localhost/afy/property/benchgraph1/orgid");
                for (var __iP = 0; __iP < __pR.length; __iP++)
                {
                  var __lP = __pR[__iP];
                  var __lToOrgid = __lP.get("http://localhost/afy/property/benchgraph1/orgid");
                  if (__lToOrgid == _mEndOrgid)
                  {
                    _mFound = true;
                    _mFinalCallback(_lGetSolution(__lFromOrgid));
                    break;
                  }
                  if (__lToOrgid in _mBwHops) { continue; }
                  _mBwHops[__lToOrgid] = __lFromOrgid;
                  _mNextLevel.push(__lP);
                }
                _pSS.next();
              });
          });
      }
      var _lRecurse = function(_pSS, _pPINs, _pLevel)
      {
        /*
        var __lTrace = "";
        for (var __j = 0; __j < _pPINs.length; __j++)
          __lTrace += _pPINs[__j].get("http://localhost/afy/property/benchgraph1/orgid") + " ";        
        console.log("level " + _pLevel + ": " + __lTrace);
        */

        // Collect all friends of the next level.
        _mNextLevel = [];
        for (var __i = 0; __i < _pPINs.length; __i++)
          _lRecurseOne(_pSS, _pPINs[__i], _pLevel);
        // Process them.
        _pSS.push(
          function()
          {
            if (_mFound || 0 == _mNextLevel.length) { _pSS.next(); return; }
            var ___lSS2 = new InstrSeq();
            _lRecurse(___lSS2, _mNextLevel.slice(0), _pLevel + 1);
            ___lSS2.push(_pSS.next);
            ___lSS2.start();
          });
      }
      this.solve = function()
      {
        var __lSS = new InstrSeq();
        var __lStartPIN;
        __lSS.push(
          function()
          {
            lAffinity.qProto(
              "SELECT * FROM \"http://localhost/afy/class/benchgraph1/orgid\"(" + _mStartOrgid + ");",
              function(_pE, _pR)
              {
                assertValidResult(_pR);
                __lStartPIN = _pR[0];
                __lSS.next();
              });
          });
        __lSS.push(
          function()
          {
            var ___lSS2 = new InstrSeq();
            _lRecurse(___lSS2, [__lStartPIN], 1);
            ___lSS2.push(__lSS.next);
            ___lSS2.start();
          });
        __lSS.push(function() { if (!_mFound) { _mFinalCallback(null); } });
        __lSS.start();
      }
    }

    // Queries - case 1: can person1 reach person2?
    lSS.push(
      function()
      {
        console.log("case 1:");
        var _lSS = new InstrSeq();
        var _lQ =
          function(_pP)
          {
            _lSS.push(
              function()
              {
                var __lP2 = lNumPeople - _pP;
                new BFSearchCtx(
                  _pP, __lP2,
                  function(__pSolution)
                  {
                    if (undefined == __pSolution) { console.log("" + _pP + " cannot reach " + __lP2); }
                    else { console.log("" + _pP + " can reach " + __lP2 + " via " + __pSolution.join(" via ")); }
                    _lSS.next(); 
                  }).solve();
              });
          }
        for (var _iP = 10; _iP < lNumPeople; _iP += 10) { _lQ(_iP); }
        _lSS.push(lSS.next);
        _lSS.start();
      });

    // Queries - case 2: find the set of all people that have friends among A's friends
    lSS.push(
      function()
      {
        console.log("case 2:");
        var _lSS = new InstrSeq();
        var _lQ =
          function(_pP)
          {
            _lSS.push(
              function()
              {
                lAffinity.q(
                  "SELECT * FROM @" + _pP.pid.toString(16) + ".\"http://localhost/afy/property/benchgraph1/friendof\".\"http://localhost/afy/property/benchgraph1/friendof\"[@ <> @" + _pP.pid.toString(16) + "];",
                  function(__pE, __pR)
                  {
                    console.log("" + __pR.length + " people have friends in common with " + _pP.get("http://localhost/afy/property/benchgraph1/firstname") + " " + _pP.get("http://localhost/afy/property/benchgraph1/middlename") + " " + _pP.get("http://localhost/afy/property/benchgraph1/lastname"));
                    _lSS.next();
                  });
              });
          }
        for (var _iP = 0; _iP < lPeopleSample.length; _iP++) { _lQ(lPeopleSample[_iP]); }
        _lSS.push(lSS.next);
        _lSS.start();
      });

    // Queries - case 3: find all photos owned by A, i.e. present in the project structure owned by A
    lSS.push(
      function()
      {
        console.log("case 3:");
        var _lSS = new InstrSeq();
        var _lQ =
          function(_pP)
          {
            _lSS.push(
              function()
              {
                lAffinity.q(
                  "SELECT * FROM @" + _pP.pid.toString(16) + ".\"http://localhost/afy/property/benchgraph1/rootproject\".\"http://localhost/afy/property/benchgraph1/children\"{*}[exists(\"http://localhost/afy/property/benchgraph1/pname\")];",
                  function(__pE, __pR)
                  {
                    console.log(_pP.get("http://localhost/afy/property/benchgraph1/firstname") + " " + _pP.get("http://localhost/afy/property/benchgraph1/middlename") + " " + _pP.get("http://localhost/afy/property/benchgraph1/lastname") + " has projects containing " + __pR.length + " photos");
                    _lSS.next();
                  });
              });
          }
        for (var _iP = 0; _iP < lPeopleSample.length; _iP++) { _lQ(lPeopleSample[_iP]); }
        _lSS.push(lSS.next);
        _lSS.start();
      });

    // Queries - case 4: find all photos given access to A by his friends
    lSS.push(
      function()
      {
        console.log("case 4:");
        var _lSS = new InstrSeq();
        var _lQ =
          function(_pP)
          {
            _lSS.push(
              function()
              {
                lAffinity.q(
                  "SELECT * FROM @" + _pP.pid.toString(16) + ".\"http://localhost/afy/property/benchgraph1/friendof\".\"http://localhost/afy/property/benchgraph1/rootproject\".\"http://localhost/afy/property/benchgraph1/children\"{*}[\"http://localhost/afy/property/benchgraph1/access\"=" + _pP.get("http://localhost/afy/property/benchgraph1/orgid") + "].\"http://localhost/afy/property/benchgraph1/children\"[exists(\"http://localhost/afy/property/benchgraph1/pname\")];",
                  function(__pE, __pR)
                  {
                    console.log(_pP.get("http://localhost/afy/property/benchgraph1/firstname") + " " + _pP.get("http://localhost/afy/property/benchgraph1/middlename") + " " + _pP.get("http://localhost/afy/property/benchgraph1/lastname") + " has access to " + __pR.length + " photos shared by friends");
                    _lSS.next();
                  });
              });
          }
        for (var _iP = 0; _iP < lPeopleSample.length; _iP++) { _lQ(lPeopleSample[_iP]); }
        _lSS.push(lSS.next);
        _lSS.start();
      });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
};
=end
