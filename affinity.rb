#!/usr/bin/env ruby
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
# This module defines the key components of Affinity's low-level client library in ruby:
# Connection and PIN (including PIN::PID and PIN::Collection).
# The library talks to the store via pathSQL, JSON and protobuf exclusively,
# using HTTP to reach the Affinity server.
# Please read the documentation of each component for more details.
# Note that this ruby implementation is essentially an adapted copy of the python one
# (with expected adjustments in terms of complying with the ruby Hash etc.).

require 'rubygems'
require 'cgi'
require 'fcntl'
require 'net/http'
require 'base64'
require 'affinity.pb'
require 'json'
require 'logger'
require 'enumerator'

module Affinity

  # Standard names for special properties.
  SP_PROPERTY_NAMES =
  {
    AffinityPB::SpecProp::SP_PINID.value => "afy:pinID",
    AffinityPB::SpecProp::SP_DOCUMENT.value => "afy:document",
    AffinityPB::SpecProp::SP_PARENT.value => "afy:parent",
    AffinityPB::SpecProp::SP_VALUE.value => "afy:value",
    AffinityPB::SpecProp::SP_CREATED.value => "afy:created",
    AffinityPB::SpecProp::SP_CREATEDBY.value => "afy:createdBy",
    AffinityPB::SpecProp::SP_UPDATED.value => "afy:updated",
    AffinityPB::SpecProp::SP_UPDATEDBY.value => "afy:updatedBy",
    AffinityPB::SpecProp::SP_ACL.value => "afy:ACL",
    AffinityPB::SpecProp::SP_URI.value => "afy:URI",
    AffinityPB::SpecProp::SP_STAMP.value => "afy:stamp",
    AffinityPB::SpecProp::SP_CLASSID.value => "afy:classID",
    AffinityPB::SpecProp::SP_PREDICATE.value => "afy:predicate",
    AffinityPB::SpecProp::SP_NINSTANCES.value => "afy:nInstances",
    AffinityPB::SpecProp::SP_NDINSTANCES.value => "afy:nDelInstances",
    AffinityPB::SpecProp::SP_SUBCLASSES.value => "afy:subclasses",
    AffinityPB::SpecProp::SP_SUPERCLASSES.value => "afy:superclasses",
    AffinityPB::SpecProp::SP_CLASS_INFO.value => "afy:classInfo",
    AffinityPB::SpecProp::SP_INDEX_INFO.value => "afy:indexInfo",
    AffinityPB::SpecProp::SP_PROPERTIES.value => "afy:properties",
  }.freeze

  #
  # PBReadCtx
  # Affinity protobuf response reading context.
  # Note:
  #   In a majority of cases the developer needs not be aware of this (PIN.loadPINs hides it).
  #
  class PBReadCtx
    attr_reader :pbStream, :propid2name, :propname2id, :identMap, :owner, :storeid
    def initialize(pbStream)
      @pbStream = pbStream
      @propid2name = {}
      @propname2id = {}
      @identMap = {}
      @owner = pbStream.owner
      @storeid = pbStream.storeID
      pbStream.properties.each do |strmap|
        @propid2name[strmap.id] = strmap.str
        @propname2id[strmap.str] = strmap.id
      end
      SP_PROPERTY_NAMES.each do |id, str|
        @propid2name[id] = str
        @propname2id[str] = id
      end
      pbStream.identities.each do |strmap|
        @identMap[strmap.id] = strmap.str
      end
    end
  end

  #
  # PIN
  # Note:
  #   Unlike in python, here I decided not to subclass Hash, because the interface is more messy;
  #   fortunately at least, ruby makes it easy to reuse the implementation of the read-methods of Hash.
  # Note:
  #   http://www.ruby-doc.org/core-1.8.7/Hash.html
  #
  class PIN
    attr_accessor :pid, :isUpdate, :extras

    # Special Keys.
    SK_PID = "__PID__"
    SK_UPDATE = "__UPD__"

    # Affinity time offset (1600 vs 1970).
    TIME_OFFSET = 11644473600000000 # would be: (Time.utc(1970,1,1) - Time.utc(1601,1,1)).days * 24 * 60 * 60 * 1000000

    # Just to facilitate debugging.
    class PINi < Hash
    end

    # In-memory representation of collections, overriding the natural 'Array' representation to track
    # changes through native/std ruby methods for arrays, and make them persistent. Not self-sufficient
    # (i.e. in symbiosis with the owning PIN object).
    # Review:
    #   Is this ruby-esque enough? Did ruby authors intentionally avoid
    #   a canonical core implementation (a la python's MutableSequence)?
    class Collection < Array
      alias :super_push :push
      alias :super_assign :[]=

      # Constructor. 
      def initialize(pin, property, *rest)
        raise "Invalid parameters #{if pin.nil? or pin.pid.nil? then 'nil' else pin.pid end}.#{property.inspect}" if (pin.nil? or !property.is_a? String)
        @pin = pin
        @property = property
        super_push(*rest)
      end

      # For debugging.
      # def inspect() "collection of #{if @pin.pid.nil? then "nil" else @pin.pid end}.#{@property}: [#{map {|v| v.inspect}.compact.join(",")}]" end

      def insert(i, *rest)
        raise "Unexpected type for i: #{i.class}" unless (i.is_a? Numeric and i.integer?)
        if i < 0 then i = (self.length + i + 1) end
        if @pin != nil
          rest.each do |v|
            # Grab the eid, and insert the corresponding 'extra'.
            if @pin.extras.has_key? @property
              extras = @pin.extras[@property]
              raise "i out of range: #{i} (#{extras.length} elements)" unless (i >= 0 and i <= extras.length)
              raise "Unexpected type: #{v.class}" if v.is_a? Array # Review: accept a PIN::Extra here?
              eid = Extra::EID_LAST_ELEMENT; op = AffinityPB::Value::ModOp::OP_ADD.value
              if i == 0
                eid = Extra::EID_FIRST_ELEMENT; op = AffinityPB::Value::ModOp::OP_ADD_BEFORE.value
              elsif i < extras.length
                eid = extras[i].eid; op = AffinityPB::Value::ModOp::OP_ADD_BEFORE.value
              end
              extras.insert(i, PIN::Extra.new)
              # Record a persistent update.
              @pin._handlePINUpdate(PIN[SK_PID=>@pin.pid, @property=>[v, PIN::Extra.collOp(op, eid)]])
            end
          end
        end
        super
      end
      def push(*rest) insert(-1, *rest) end
      def <<(elm) push(elm) end
      def concat(other) other.each do |v| push(v) end; end

      def pop(*rest)
        n = if rest.length > 0 then rest[0] else 1 end
        raise "Invalid argument #{n}" if n <= 0
        if @pin != nil and @pin.extras.has_key? @property
          extras = @pin.extras[@property]
          raise "Invalid argument n=#{n}" if n > extras.length
          (extras.length - 1).downto(extras.length - n) do |iE|
            eid = extras.pop().eid
            @pin._handlePINUpdate(PIN[SK_PID=>@pin.pid, @property=>[0, PIN::Extra.collOp(AffinityPB::Value::ModOp::OP_DELETE.value, eid)]])
          end
        end
        super
      end

      def []=(*rest)
        raise "Unexpected number of arguments" unless rest.length >=2 and rest.length <=3
        at = if rest.length == 2 then rest[0] else rest[0]..(rest[0] + rest[1] - 1) end
        value = if rest.length == 2 then rest[1] else rest[2] end
        if at.is_a? Range
          raise "Unexpected range type #{at.begin.class}" unless at.begin.is_a? Numeric and at.begin.integer?
          at.end.downto(at.begin) do |index| delete_at(index) end
          insert(at.begin, value)
          return value
        elsif at.is_a? Numeric and at.integer? and @pin != nil and @pin.extras.has_key? @property
          @pin._handlePINUpdate(PIN[SK_PID=>@pin.pid, @property=>[value, PIN::Extra.collOp(AffinityPB::Value::ModOp::OP_SET.value, @pin.extras[@property][at].eid)]])
          return super
        end
        raise "Invalid arguments or state"
      end

      def clear()
        @pin.delete(@property)
        super
      end

      def delete(value, &block)
        toDelete = []
        self.each_index do |i| if self[i] == value then toDelete << i end; end
        if toDelete.length > 0
          (toDelete.length - 1).downto(0) do |i| delete_at(toDelete[i]) end
          value
        elsif block.nil?
          nil
        else
          yield
        end
      end

      def delete_at(index)
        raise "Invalid argument #{index}" unless index.is_a? Numeric and index.integer? and index >= 0
        if @pin != nil
          # Grab the eid, and remove the corresponding 'extra'.
          if @pin.extras.has_key? @property
            extras = @pin.extras[@property]
            raise "index out of range: #{index} (#{extras.length} elements)" if index >= extras.length
            eid = extras[index].eid
            extras.delete_at(index)
            # Record a persistent update.
            @pin._handlePINUpdate(PIN[SK_PID=>@pin.pid, @property=>[0, PIN::Extra.collOp(AffinityPB::Value::ModOp::OP_DELETE.value, eid)]])
          end
        end
        super
      end

      def delete_if(&block)
        if block.nil?
          self.enum_for(:_delete_if_impl)
        else
          _delete_if_impl(&block); self
        end
      end
      def _delete_if_impl(&block) self.each_index do |i| if yield self[i] then delete_at(i) end; end; end
      def reject!(&block) delete_if(&block) end

      def keep_if(&block)
        if block.nil?
          self.enum_for(:_keep_if_impl)
        else
          _keep_if_impl(&block); self
        end
      end
      def _keep_if_impl(&block) self.each_index do |i| if !(yield self[i]) then delete_at(i) end; end; end
      def select!(&block) keep_if(&block) end

      def uniq!()
        unique = {}; uniqueIdx = {}; changed = false
        self.each_index do |i| if !unique.has_key? self[i] then unique[self[i]] = i; uniqueIdx[i] = 1; end; end
        (self.length - 1).downto(0) do |i|
          if !uniqueIdx.has_key? i then delete_at(i); changed = true; end
        end
        if changed then self else nil end
      end

      def slice!(*rest)
        from = 0; to = self.length - 1
        if 1 == rest.length
          if rest[0].is_a? Range
            from = rest[0].begin; to = rest[0].end
          else
            from = to = rest[0]
          end
        elsif 2 == rest.length
          from = rest[0]; to = rest[0] + rest[1] -1
        else
          raise "Unexpected number of arguments: #{rest.length}"
        end
        result = []
        (self.length - 1).downto(0) do |i|
          if i >= from and i <= to
            result << delete_at(i)
          end
        end
        result
      end

      def fill(*rest, &block)
        _self = self
        _bounds = lambda\
        {
          |arguments|
          _from = 0; _to = _self.length - 1
          if 0 == arguments.length
          elsif 1 == arguments.length
            if arguments[0].is_a? Range
              _from = arguments[0].begin; _to = arguments[0].end 
            elsif !arguments[0].nil?
              _from = arguments[0]
            end
          elsif 2 == arguments.length
            _from = arguments[0]; _to = arguments[0] + arguments[1] - 1
          else
            raise "Unexpected number of specifiers: #{arguments.length}"
          end
          return _from, _to
        }
        from, to = _bounds.call(if block.nil? then rest.drop(1) else rest end)
        if block.nil?
          from.upto(to) do |i| self[i] = rest[0] end
        else
          from.upto(to) do |i| self[i] = yield i end
        end
        self
      end

      def unshift(*rest)
        if 0 == rest.length then return self end
        (rest.length - 1).downto(0) do |i| insert(0, rest[i]) end
        self
      end

      def collect!(&block)
        raise "Unexpected" if block.nil?
        self.each_index do |i| self[i] = yield self[i] end
        self
      end
      def map!(&block) collect!(&block) end

      def compact!() self end
      def flatten!(*rest) nil end
      def replace(other) clear; concat(other) end

      def reverse!()
        toProcess = _shuffle_begin()
        toProcess.reverse! # Note: reversing toProcess is equivalent to reversing self.
        _shuffle_end(toProcess)
        self
      end

      def rotate!(cnt=1)
        toProcess = _shuffle_begin()
        toProcess.rotate!(cnt) # Note: rotating toProcess is equivalent to rotating self.
        _shuffle_end(toProcess)
        self
      end

      def shuffle!(*rest)
        toProcess = _shuffle_begin()
        toProcess.shuffle!(*rest) # Note: shuffling toProcess is equivalent to shuffling self.
        _shuffle_end(toProcess)
        self
      end

      def sort!(&block)
        toProcess = _shuffle_begin()
        if block.nil?
          toProcess.sort! {|a,b| a[0] <=> b[0]}
        else
          toProcess.sort! {|a,b| yield a[0], b[0]}
        end
        _shuffle_end(toProcess)
        self
      end

      def sort_by!(&block)
        toProcess = _shuffle_begin()
        toProcess.sort_by!(&block) # Not perfect... review.
        _shuffle_end(toProcess)
        self
      end

      private :_delete_if_impl
      private :_keep_if_impl
      private

      # Generic handling of all shuffling-only in-place modifiers (e.g. sort!).
      def _shuffle_begin()
        # Collect and return a copy of the list of values to operate on, paired with their eids and original index.
        0.upto(self.length - 1).map{|i| [self[i], @pin.extras[@property][i].eid, i]}.compact()
      end
      def _shuffle_end(processed)
        # Apply the resulting ordering to the persistent state (in the context of the current transaction).
        extras = @pin.extras[@property]
        prevEid = Extra::EID_FIRST_ELEMENT
        processed.each_index do |i|
          iTuple = processed[i]
          if extras[i].eid == iTuple[1]
            # This element is already at the right place - don't touch it.
            prevEid = iTuple[1]
            next
          end
          # Keep the in-memory 'extras' in sync with the persistent updates we're recording.
          extras.insert(i, extras.delete_at(iTuple[2]))
          # Record a persistent update.
          op = if prevEid == Extra::EID_FIRST_ELEMENT then AffinityPB::Value::ModOp::OP_MOVE_BEFORE.value else AffinityPB::Value::ModOp::OP_MOVE.value end
          @pin._handlePINUpdate(PIN[SK_PID=>@pin.pid, @property=>[prevEid, PIN::Extra.new(nil, AffinityPB::Value::ValueType::VT_UINT.value, op, iTuple[1])]])
          prevEid = iTuple[1]
        end
        # Update the actual values.
        processed.each_index do |i|
          super_assign(i, processed[i][0])
        end
      end
    end

    # Semi-hidden representation of all the Affinity adornments on a plain value (e.g. eid, meta, type, op, etc.).
    # This allows to present the PIN as a simple dictionary where keys are property names, and values are native ruby values.
    # Everything else is hidden as 'extras', and used mostly transparently when needed.
    class Extra
      attr_accessor :eid
      attr_reader :propID, :vtype, :op, :meta
      OP_NAMES =
        ["OP_SET", "OP_ADD", "OP_ADD_BEFORE", "OP_MOVE", "OP_MOVE_BEFORE", "OP_DELETE", "OP_EDIT", "OP_RENAME",
          "OP_PLUS", "OP_MINUS", "OP_MUL", "OP_DIV", "OP_MOD", "OP_NEG", "OP_NOT", "OP_AND", "OP_OR", "OP_XOR",
          "OP_LSHIFT", "OP_RSHIFT", "OP_MIN", "OP_MAX", "OP_ABS", "OP_LN", "OP_EXP", "OP_POW", "OP_SQRT",
          "OP_FLOOR", "OP_CEIL", "OP_CONCAT", "OP_LOWER", "OP_UPPER", "OP_TONUM",
          "OP_TOINUM", "OP_CAST"].freeze # Review: could this be done with introspection?
      VT_NAMES =
        ["VT_ANY",
          "VT_INT", "VT_UINT", "VT_INT64", "VT_UINT64",
          "VT_RESERVED2", "VT_FLOAT", "VT_DOUBLE", "VT_BOOL",
          "VT_DATETIME", "VT_INTERVAL",
          "VT_URIID", "VT_IDENTITY",
          "VT_STRING", "VT_BSTR", "VT_URL", "VT_RESERVED1",
          "[undefined-17]",
          "VT_REFID", "[undefined-19]", "VT_REFIDPROP", "[undefined-21]", "VT_REFIDELT", "VT_EXPR", "VT_STMT",
          "VT_ARRAY", "[undefined-26]", "VT_STRUCT", "VT_RANGE", "[undefined-29]", "VT_CURRENT", "VT_VARREF", "[undefined-32]"].freeze # Review: can this be done with introspection?
      EID_COLLECTION = 4294967295 # AffinityPB::SpecEID::EID_COLLECTION.value, but in the positive range...
      EID_LAST_ELEMENT = 4294967294
      EID_FIRST_ELEMENT = 4294967293
      def initialize(propID=nil, vtype=AffinityPB::Value::ValueType::VT_ANY.value, op=AffinityPB::Value::ModOp::OP_SET.value, eid=EID_COLLECTION, meta=0)
        @propID = propID # Conceptually redundant with the key, but kept for efficiency, since Affinity doesn't require a StringMap for existing propids.
        @vtype = vtype # There are cases where a single native ruby value type covers multiple Affinity VT types... so we keep the actual specific type for future updates.
        @op = op
        @eid = eid
        @meta = meta
      end
      def to_s() "#{OP_NAMES[@op]}:#{VT_NAMES[@vtype]}:#{@eid}" end
      def inspect() to_s end
      def Extra.fromPB(pbValue) Extra.new(pbValue.property, pbValue.type, pbValue.op, pbValue.eid, pbValue.meta) end
      def Extra.collOp(op=AffinityPB::Value::ModOp::OP_SET.value, eid=EID_COLLECTION) Extra.new(nil, AffinityPB::Value::ValueType::VT_ANY.value, op, eid) end
    end

    # PID native (non-PB) representation.
    class PID
      include Comparable
      attr_reader :localPID, :ident
      def initialize(localPID, ident=0)
        raise "Invalid Parameter" unless (localPID.integer? and ident.integer?)
        @localPID = localPID # 64-bit unsigned integer.
        @ident = ident # 32-bit unsigned integer.
      end
      def <=>(other)
        if nil == other then return 1 end
        o = other
        if o.is_a? String then return to_s <=> o end
        if o.is_a? PIN then o = o.pid end
        if @localPID != o.localPID then return @localPID <=> o.localPID end
        if @ident != o.ident then return @ident <=> o.ident end
        0
      end
      def to_s() "@#{@localPID.to_s(16)}" end
      def inspect() to_s end
      def PID.fromPB(pbPID) PID.new(pbPID.id, pbPID.ident) end
    end

    # PIN reference native (non-PB) representation.
    class Ref < PID
      attr_reader :property, :eid
      def initialize(localPID, ident=0, property=nil, eid=nil)
        super(localPID, ident)
        @property = property # Property name, or nil.
        @eid = eid # 32-bit unsigned integer, or nil.
      end
      def <=>(other)
        s = super(other)
        if 0 != s then return s end
        o = other
        if o.is_a? PIN then o = o.pid end
        if @property != o.property then return @property <=> o.property end
        if @eid != o.eid then return @eid <=> o.eid end
        0
      end
      def to_s
        if @eid != nil
          "@#{localPID.to_s(16)}.#{@property}[#{@eid}]"
        elsif @property != nil
          "@#{localPID.to_s(16)}.#{@property}"
        else
          super
        end
      end
      def inspect() to_s end
      def Ref.fromPID(pid) Ref.new(pid.localPID, pid.ident) end
    end

    # To help distinguish a URI from a plain string.
    class Url < String
    end

    # To help distinguish a byte array from a plain string.
    class ByteArray < String
    end

    # Special marker on PIN objects, to identify that they represent updates, not the full PIN.
    # Also allows to track other in-memory instances, and update them according to the changes effected by this update.
    # Currently, we only support a single 'other' PIN, and only for eid inserts.
    class PINUpdate
      def initialize(otherPINs)
        raise "Invalid type: #{otherPINs.class}" unless otherPINs.is_a? Array
        @otherPINs = otherPINs
      end
    end

    # Constructor.
    # TODO: make sure all typical ruby combinations are covered (e.g. try_convert etc.)
    def initialize()
      @pid = nil
      @hash = PINi.new
      @extras = {}
      @updateOf = nil
    end

    def PIN.[](*rest)
      raise "Unexpected parameter: #{rest.inspect}" unless rest.length > 0
      pin = PIN.new
      h = {}
      if rest.length == 1 and rest[0].is_a? Hash
        h = rest[0]
      elsif rest.length.even? and !rest[0].is_a? Array
        0.upto(rest.length / 2 - 1) {|i| h[rest[i * 2]] = rest[i * 2 + 1]}
      elsif rest[0].is_a? Array and rest[0].length == 2
        rest.each do |pair| h[pair[0]] = pair[1] end
      else
        raise "Unexpected Parameter: #{rest.inspect}" if pin.nil?
      end
      pid = nil
      if h.has_key? SK_PID then pid = h[SK_PID]; h.delete(SK_PID); end
      pin.replace(h)
      pin.pid = pid # Note: must be done at the end, to avoid infinite save-create-save-create...
      #puts "### created #{pin.inspect}"
      pin
    end

    def yaml_initialize(tag, val)
      raise "Affinity:PIN.yaml_initialize: Not yet implemented."
    end

    #--------
    # PUBLIC: Core Hash implementation - write.
    # TODO: Make sure all write methods are covered.
    #--------

    def store(key, value) _assign(key, value) end
    def []=(key, value) _assign(key, value) end

    def update(h, &block)
      if block.nil?
        h.each do |k, v| store(k, v) end
      else
        h.each do |k, v|
          if @hash.has_key? k
            store(k, yield(k, @hash[k], v))
          else
            store(k, v)
          end
        end
      end
      self
    end
    alias merge! update

    def replace(h)
      oldKeys = {}
      @hash.each_key do |k| oldKeys[k] = 1; end
      h.each do |k, v| store(k, v); oldKeys.delete(k); end
      oldKeys.each_key do |k| delete(k); end
      self
    end

    def delete(key, &block)
      r = if block.nil? then @hash.delete(key) else @hash.delete(key) do yield; end; end
      if @extras.has_key?(key) then @extras.delete(key); end
      if @pid != nil then _handlePINUpdate(PIN[SK_PID=>@pid, key=>[0, PIN::Extra.collOp(AffinityPB::Value::ModOp::OP_DELETE.value)]]); end
      r
    end

    def delete_if(&block)
      if block.nil?
        self.enum_for(:_delete_if_impl)
      else
        @hash.delete_if do |k, v|
          if yield k, v
            # TODO: implement
            true
          else
            false
          end
        end
      end
    end
    alias reject! delete_if # Review: correct?

    def shift()
      key = nil
      @hash.each_key do |k| key = k; break end
      delete(key)
    end

    def clear()
      @hash.each_key do |k| delete(k) end
      self
    end

    #--------
    # PUBLIC: Core Hash implementation - enumerators (delegated to our internal @hash).
    #--------

    include Enumerable
    def each(&block) if block.nil? then @hash.each else @hash.each do |k, v| yield k, v; end; end; end
    alias each_pair each
    def each_value(&block) if block.nil? then @hash.each_value else @hash.each_value do |v| yield v; end; end; end
    def each_key(&block) if block.nil? then @hash.each_key else @hash.each_key do |k| yield k; end; end; end
    def sort(&block) if block.nil? then @hash.sort else @hash.sort do |k, v| yield k, v; end; end; end
    def select(&block) if block.nil? then @hash.select else @hash.select do |k, v| yield k, v; end; end; end
    alias indexes select # Review: correct?
    alias indices select # Review: correct?
    def merge(h, &block) if block.nil? then @hash.merge(h) else @hash.merge(h) do |k, ov, nv| yield k, ov, nv; end; end; end
    def fetch(k, *rest, &block) if block.nil? then @hash.fetch(k, rest) else @hash.fetch(k) do |k| yield k; end; end; end

    #--------
    # PUBLIC: Direct access to 'Extras'.
    #--------

    def getExtra(property, eid=Extra::EID_COLLECTION)
      # Return the 'Extra' structure associated with the value specified by property and eid.
      raise "Invalid property #{property}" unless @extras.has_key? property
      extras = @extras[property]
      if eid != Extra::EID_COLLECTION
        i = extras.index {|item| item.eid == eid}
        if !i.nil?
          return extras[i]
        end
      elsif 1 == extras.length
        return extras[0]
      end
      raise "Invalid eid=#{eid}"
    end

    def markAsUpdate(update)
      raise "Invalid parameter type: #{update.class}" unless update.is_a? PIN::PINUpdate
      @updateOf = update
    end

    #--------
    # PUBLIC: PIN saving (to Affinity). Note: At this level of the API, a PIN can represent a whole Affinity PIN, or just a set of updates to be applied on an actual Affinity PIN.
    #--------

    def PIN.savePINs(pins, txCtx=nil)
      # Save pins to the store.
      if 0 == pins.length
        return
      end
      # Serialize, and request an immediate response (synchronous reception of resulting PIDs, eids etc.).
      txCtx = txCtx || Connection.getCurrentDbConnection._txCtx()
      txCtx.logger.debug("saving #{if @pid.nil? then "new PIN" else @pid end}")
      begin
        PIN._savePINsi(pins, txCtx)
        txCtx.flush()
      rescue => ex
        txCtx.logger.error("Exception #{ex}: #{ex.backtrace}")
        raise
      end
      # Handle errors.
      if txCtx.rc.nil?
        txCtx.logger.warn("failed to save PINs.")
        return nil
      end
      # Determine if we need to process the output.
      processOutput = !txCtx.isOutputIgnored
      if !processOutput
        pins.each do |pin|
          if pin.isUpdate and pin.isUpdate.otherPINs.length > 0
            processOutput = true
            break
          end
        end
      end
      if !processOutput or !txCtx.pbOutput
        return pins
      end
      # Obtain the resulting IDs generated by Affinity.
      if txCtx.pbOutput.pins.length != pins.length
        txCtx.logger.warn("#{pins.length} PINs were saved, but response contained only #{txCtx.pbOutput.pins.length} PINs.")
      end
      readCtx = PBReadCtx.new(txCtx.pbOutput)
      txCtx.pbOutput.pins.each_with_index do |pin, ipin|
        # Substitute iPin if it's an update PIN on an identified actual PIN.
        # Review: lots of potential improvements and verifications...
        thepin = pins[ipin]
        if thepin.isUpdate and thepin.isUpdate.otherPINs.length > 0
          thepin = thepin.isUpdate.otherPINs[0]
        end
        # The PID.
        if thepin.pid.nil?
          thepin.pid = PID.new(pin.id.id, pin.id.ident)
        else
          raise "Expected PID #{thepin.pid.localPID} but received #{pin.id.id}" unless thepin.pid.localPID == pin.id.id
        end
        # The eids.
        pin.values.each do |v|
          propName = readCtx.propid2name()[v.property]
          extra = thepin.extras[propName]
          if AffinityPB::Value::ValueType::VT_ARRAY == v.type
            v.varray.v.each_with_index do |elm, ielm|
              if [Extra::EID_COLLECTION, Extra::EID_LAST_ELEMENT, Extra::EID_FIRST_ELEMENT].include? extra[ielm].eid
                extra[ielm].eid = elm.eid
                txCtx.logger.debug("obtained eid=#{elm.eid} (#{propName})")
              end
            end
          elsif [AffinityPB::Value::ModOp::OP_ADD, AffinityPB::Value::ModOp::OP_ADD_BEFORE].include? extra[0].op
            extra[0].eid = v.eid
            txCtx.logger.debug("obtained eid=#{v.eid} (#{propName})")
          else
            txCtx.logger.debug("didn't obtain eid (#{propName}): #{v.inspect}")
          end
        end
      end
      txCtx.clearPBOutput
      return pins
    end

    def savePIN(txCtx=nil)
      # Save self to the store.
      PIN.savePINs([self], txCtx)
      self
    end

    #--------
    # PUBLIC: PIN deletion (from Affinity).
    #--------
    # TODO: soft vs purge, undelete etc.

    def PIN.deletePINs(pids, txCtx=nil)
      # Delete pids from the store.
      txCtx = txCtx || Connection.getCurrentDbConnection._txCtx()
      pids.each do |pid|
        raise "Invalid parameter type: #{pid.class}" unless pid.is_a? PID
        pbPin = AffinityPB::AfyStream::PIN.new
        pbPin.id = AffinityPB::PID.new
        pbPin.id.id = pid.localPID
        pbPin.id.ident = pid.ident
        pbPin.op = AffinityPB::AfyStream::MODOP::OP_DELETE
        txCtx.getPBStream().pins.push(pbPin)
      end
      txCtx.flush(false)
      # TODO: Decide if final confirmation is a RC or an exception.
    end

    def deletePIN(txCtx=nil)
      # Delete self from the store.
      raise "Invalid state: #{@pid}" if @pid.nil?
      PIN.deletePINs([@pid], txCtx)
    end

    #--------
    # PUBLIC: PIN loading/refreshing (from Affinity).
    #--------

    def PIN.loadPINs(pbStream)
      readCtx = PBReadCtx.new(pbStream)
      readCtx.pbStream.pins.map {|pbPin| PIN.new().loadPIN(readCtx, pbPin)}.compact
    end

    def loadPIN(readCtx, pbPin)
      clearPIN()
      pbPin.values.each do |v|
        propName = readCtx.propid2name()[v.property]
        self[propName] = PIN._valuePB2RB(readCtx, v, Connection.getCurrentDbConnection.logger)
      end
      # Assign last, to avoid unwanted persistent update requests.
      @pid = PID.new(pbPin.id.id, pbPin.id.ident)
      self
    end

    def refreshPIN()
      # Refresh the contents of self, by rereading the whole PIN from the store.
      if @pid.nil? then return self end
      pbStream = Connection.getCurrentDbConnection.qProto("SELECT FROM #{@pid};")
      loadPIN(PBReadCtx.new(pbStream), pbStream.pins[0])
    end

    def clearPIN
      @pid = nil # Assign first, since the intent is not to clear the persisted PIN.
      clear()
      @extras.clear()
      # Review: @updateOf?
    end

    #--------
    # PUBLIC: Core Hash implementation - read (delegated to our internal @hash).
    #--------

    #def inspect() @hash.inspect.sub(/^\{/, "{:pid=>\"#{@pid.to_s}\", :extras=>#{@extras.inspect}, ") end
    def inspect() @hash.inspect.sub(/^\{/, "{:pid=>\"#{@pid.to_s}\", ") end

    HASH_RO_METHODS = Hash.instance_methods(false) - PIN.instance_methods(false)
    HASH_RO_METHODS.each do |m|
      #puts "DEFINING PIN method #{m} by simple delegation"
      define_method(m) do |*args|
        #puts "### delegating #{m} to internal @hash"
        @hash.send(m, *args)
      end
    end

    #--------
    # PRIVATE: Core Hash implementation.
    #--------

    def _assign(key, value)
      if key.is_a? Symbol then key = key.to_s end # Note: At input time we accept symbols as property names; but at output time we'll always produce strings.
      if value.is_a? Array
        if 2 == value.length and value[1].is_a? PIN::Extra
          @hash.store(key, value[0])
          @extras.store(key, [value[1]])
        elsif 1 <= value.length and value[0].is_a? Array
          raise "Invalid parameter type: #{value[0][1].class}" unless value[0][1].is_a? PIN::Extra
          @hash.store(key, PIN::Collection.new(self, key, *(value.map{|iP| iP[0]}.compact)))
          @extras.store(key, value.map{|iP| iP[1]}.compact)
        else
          @hash.store(key, PIN::Collection.new(self, key, *value))
          @extras[key] = []
          value.each do |v|
            @extras[key] << PIN::Extra.collOp(AffinityPB::Value::ModOp::OP_ADD.value, Extra::EID_LAST_ELEMENT)
          end
        end
      else
        @hash.store(key, value)
        @extras.store(key, [PIN::Extra.new])
      end
      if @pid != nil
        _handlePINUpdate(PIN[SK_PID=>@pid, key=>value])
      end
    end

    def _delete_if_impl(&block)
      @hash.each do |k, v| if yield k, v then delete(k) end; end
    end

    #--------
    # PRIVATE: Protobuf.
    #--------

    def PIN._insertsCollectionElements(pbPin)
      pbPin.values.each do |v|
        if [AffinityPB::Value::ModOp::OP_ADD.value, AffinityPB::Value::ModOp::OP_ADD_BEFORE.value].include?(v.op)
          return true
        end
      end
      false
    end

    def PIN._savePINsi(pins, txCtx)
      raise "Invalid parameter: #{pins.length} pins, txCtx=#{txCtx}" if (0 == pins.length or txCtx.nil?)
      # Prepare the StringMap of properties.
      propDictLenOrg = txCtx.propDict.keys().length
      pins.each do |pin|
        pin._preparePBPropIDs(txCtx)
      end
      if txCtx.propDict.keys().length > propDictLenOrg
        txCtx.capture()
      end
      # Serialize the PINs.
      pins.each do |pin|
        pbPin = AffinityPB::AfyStream::PIN.new
        if !pin.pid.nil?
          pbPin.op = AffinityPB::AfyStream::MODOP::OP_UPDATE
          pbPin.id = AffinityPB::PID.new
          pbPin.id.id = pin.pid.localPID
          pbPin.id.ident = pin.pid.ident
        else
          pbPin.op = AffinityPB::AfyStream::MODOP::OP_INSERT
        end
        pin._preparePBValues(txCtx, pbPin)
        pbPin.rtt = PIN._insertsCollectionElements(pbPin) ? AffinityPB::ReturnType::RT_PINS : AffinityPB::ReturnType::RT_PIDS
        pbPin.nValues = pbPin.values.length
        txCtx.pbStream.pins.push(pbPin)
      end
      if pins.length > 0
        txCtx.capture()
      end
    end

    def _preparePBPropIDs(txCtx)
      # [internal] Extract the StringMap of this PIN's properties, and merge it into pTxCtx.mPropDict.
      prepid =
        lambda {|_propName|
          if !txCtx.propDict.key?(_propName)
            _sm = AffinityPB::AfyStream::StringMap.new
            _sm.str = _propName
            _sm.id = (AffinityPB::SpecProp::SP_MAX.value + 1) + txCtx.propDict.length
            txCtx.pbStream.properties.push(_sm)
            txCtx.propDict[_sm.str] = _sm.id
          end
        }
      preprefid =
        lambda {|_value|
          if _value.is_a? PIN::Ref and !_value.property.nil?
            prepid.call(_value.property)
          end
        }
      # Add all the PIN's properties to the StringMap.
      # Review: Use the mPropID of mExtras, when possible... maybe...
      self.keys().each do |propName|
        prepid.call(propName)
      end
      # If there are property references, account for them.
      self.values().each do |propVal|
        if propVal.is_a? Array
          propVal.each do |elm|
            preprefid.call(elm)
          end
        else
          preprefid.call(propVal)
        end
      end
    end

    def _preparePBValues(txCtx, pbPin)
      # [internal] Add mvstore_pb2.Value objects to pPBPin, representing self.items().
      prep =
        lambda {|_propName, _extra, _rbVal|
          _v = AffinityPB::Value.new
          _v.property = 0
          if !_extra.propID.nil?
            _v.property = _extra.propID
          end
          if !_extra.vtype.nil?
            _v.type = _extra.vtype
          end
          _v.op = _extra.op
          _v.eid = _extra.eid
          _v.meta = _extra.meta
          PIN._valueRB2PB(_v, _rbVal, txCtx.propDict, txCtx.logger)
          if 0 == _v.property
            _v.property = txCtx.propDict[_propName]
          end
          pbPin.values.push(_v)
        }
      self.keys().each do |k|
        raise "Unexpected key/property type #{k.class}" unless k.is_a? String
        pv = self[k]
        extra = if @extras.has_key? k then @extras[k] else Extra.new() end
        # Collection.
        if pv.is_a? Array
          raise Exception.new() if !extra.is_a? Array
          pv.each_with_index do |elm, ielm|
            prep.call(k, extra[ielm], elm)
          end
        # Scalar.
        else
          prep.call(k, extra[0], pv)
        end
      end
    end

    def PIN._valueRB2PB(pbValue, rbValue, propDict, logger)
      # [internal] Convert a native ruby value (rbValue) into a AffinityPB::Value. If rbValue is a tuple containing an 'Extra' description, use every available field.
      type = pbValue.type
      pbValue.type = AffinityPB::Value::ValueType::VT_ANY
      if rbValue.is_a? PIN::Url
        pbValue.str = rbValue
        type = AffinityPB::Value::ValueType::VT_URL
      elsif rbValue.is_a? Symbol
        pbValue.str = rbValue.to_s
        type = AffinityPB::Value::ValueType::VT_STRING
      elsif rbValue.is_a? ByteArray
        pbValue.bstr = rbValue
        type = AffinityPB::Value::ValueType::VT_BSTR
      elsif rbValue.is_a? String
        pbValue.str = rbValue
        type = AffinityPB::Value::ValueType::VT_STRING
      elsif rbValue == true or rbValue == false
        pbValue.b = rbValue
        type = AffinityPB::Value::ValueType::VT_BOOL
      elsif rbValue.is_a? Numeric and rbValue.integer?
        # If we already know the type, don't mess with it.
        # Review: There might be cases where the new value is voluntarily not compatible; for now, expect explicit type spec in such case.
        if type == AffinityPB::Value::ValueType::VT_INT
          pbValue.i = rbValue
        elsif type == AffinityPB::Value::ValueType::VT_UINT
          pbValue.ui = rbValue
        elsif type == AffinityPB::Value::ValueType::VT_INT64
          pbValue.i64 = rbValue
        elsif type == AffinityPB::Value::ValueType::VT_UINT64
          pbValue.ui64 = rbValue
        # Otherwise, guess.
        elsif (rbValue >= -2147483648 and rbValue <= 2147483647)
          pbValue.i = rbValue
          type = AffinityPB::Value::ValueType::VT_INT
        elsif rbValue >= 0 and rbValue <= 4294967295
          pbValue.ui = rbValue
          type = AffinityPB::Value::ValueType::VT_UINT
        elsif rbValue <= 9223372036854775807
          pbValue.i64 = rbValue
          type = AffinityPB::Value::ValueType::VT_INT64
        else
          pbValue.ui64 = rbValue
          type = AffinityPB::Value::ValueType::VT_UINT64
        end
      elsif rbValue.is_a? Float
        if type == AffinityPB::Value::ValueType::VT_FLOAT
          pbValue.f = rbValue
        else
          pbValue.d = rbValue
          type = AffinityPB::Value::ValueType::VT_DOUBLE
        end
      elsif rbValue.is_a? PIN::Ref
        if !rbValue.eid.nil?
          pbValue.ref = AffinityPB::Ref.new
          pbValue.ref.id = AffinityPB::PID.new
          pbValue.ref.id.id = rbValue.localPID
          pbValue.ref.id.ident = rbValue.ident
          pbValue.ref.property = propDict[rbValue.property]
          pbValue.ref.eid = rbValue.eid
          type = AffinityPB::Value::ValueType::VT_REFIDELT
        elsif !rbValue.property.nil?
          pbValue.ref = AffinityPB::Ref.new
          pbValue.ref.id = AffinityPB::PID.new
          pbValue.ref.id.id = rbValue.localPID
          pbValue.ref.id.ident = rbValue.ident
          pbValue.ref.property = propDict[rbValue.property]
          type = AffinityPB::Value::ValueType::VT_REFIDPROP
        else
          pbValue.id = AffinityPB::PID.new
          pbValue.id.id = rbValue.localPID
          pbValue.id.ident = rbValue.ident
          type = AffinityPB::Value::ValueType::VT_REFID
        end
      # TODO: support VT_INTERVAL; support ruby Date and DateTime.
      elsif rbValue.is_a? Time
        pbValue.datetime = (1000000.0 * (rbValue.to_i + rbValue.utc_offset) + rbValue.usec + PIN::TIME_OFFSET).to_i
        type = AffinityPB::Value::ValueType::VT_DATETIME
      else
        logger.warn("Value type not yet supported: #{rbValue.class} (#{rbValue})")
      end
      if pbValue.type == AffinityPB::Value::ValueType::VT_ANY
        pbValue.type = type
      end
    end

    def PIN._valuePB2RB(readCtx, pbValue, logger)
      # [internal] Convert a AffinityPB::Value into either a single (native ruby value, Extra), or a list of them (if pbValue is a collection), and return it.
      if pbValue.type == AffinityPB::Value::ValueType::VT_ARRAY.value
        r = []
        pbValue.varray.v.each do |cv| r << PIN._valuePB2RB(readCtx, cv, logger); end
        return r
      end
      extra = Extra.fromPB(pbValue)
      if pbValue.type == AffinityPB::Value::ValueType::VT_URL.value
        [Url.new(pbValue.str), extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_STRING.value
        [pbValue.str, extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_BSTR.value
        [ByteArray.new(pbValue.bstr), extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_INT.value
        [pbValue.i, extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_UINT.value
        [pbValue.ui, extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_INT64.value
        [pbValue.i64, extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_UINT64.value
        [pbValue.ui64, extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_FLOAT.value
        [pbValue.f, extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_DOUBLE.value
        [pbValue.d, extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_BOOL.value
        [pbValue.b, extra]
      # TODO: support VT_INTERVAL.
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_DATETIME.value
        [Time.at((pbValue.datetime - PIN::TIME_OFFSET) / 1000000.0), extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_QUERY.value
        [pbValue.str, extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_REFID.value
        [PIN::Ref.new(pbValue.id.id, pbValue.id.ident), extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_REFIDPROP.value
        [PIN::Ref.new(pbValue.ref.id.id, pbValue.ref.id.ident, readCtx.propid2name()[pbValue.ref.property]), extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_REFIDELT.value
        [PIN::Ref.new(pbValue.ref.id.id, pbValue.ref.id.ident, readCtx.propid2name()[pbValue.ref.property], pbValue.ref.eid), extra]
      elsif pbValue.type == AffinityPB::Value::ValueType::VT_URIID.value
        v = readCtx.propid2name()[pbValue.ui]
        if v == nil
          v = pbValue.ui
          logger.warn("Could not resolve VT_URIID #{pbValue.ui}")
        end
        return [v, extra]
      else
        logger.warn("Unknown value type #{pbValue.type}")
        nil
      end
    end

    def _handlePINUpdate(pinUpdate, txCtx=nil)
      txCtx = txCtx || Connection.getCurrentDbConnection._txCtx()
      pinUpdate.markAsUpdate(PIN::PINUpdate.new([self]))
      if txCtx and not txCtx.performImmediateUpdates() and 0 != txCtx.txCnt
        txCtx.recordPINUpdate(pinUpdate)
      else
        pinUpdate.savePIN(txCtx)
      end
    end
  end

  #
  # Connection
  #
  class Connection
    attr_reader :logger

    def Connection.getCurrentDbConnection()
      if Thread.current[:AffinityConnectionStack].nil?
        Thread.current[:AffinityConnectionStack] = [@@defaultConnection]
        puts "default connection: #{@@defaultConnection}"
      end
      Thread.current[:AffinityConnectionStack][-1]
    end
    def Connection.pushDbConnection(connection)
      if Thread.current[:AffinityConnectionStack].nil?
        Thread.current[:AffinityConnectionStack] = [@@defaultConnection]
      end
      oldc = Thread.current[:AffinityConnectionStack][-1]
      puts "### pushed connection: #{connection}"
      Thread.current[:AffinityConnectionStack] << connection
      oldc
    end
    def Connection.popDbConnection(connection)
      if Thread.current[:AffinityConnectionStack].nil?
        return nil
      end
      raise "Connection stack imbalance" unless connection == Thread.current[:AffinityConnectionStack][-1]
      Thread.current[:AffinityConnectionStack].delete_at(-1)
    end

    # Transaction context for protobuf. Allows to run long transactions and fetch intermediate results
    # (without any dependency on a keep-alive connection). Facilitates the concatenation of protobuf logical AfyStream segments,
    # to produce the final outgoing stream.
    class PBTransactionCtx
      attr_reader :pbStream, :rc, :pbOutput, :propDict, :txCnt
      MODE_IGNORE_OUTPUT = 0x0001
      MODE_IMMEDIATE_UPDATES = 0x0002
      @@nextCID = 1 # Review: thread-safety
      # ---
      def initialize(connection=nil, mode=0)
        @connection = connection || Connection.getCurrentDbConnection()
        puts "new txCtx: connection=#{@connection.class.name}"
        @pbStream = AffinityPB::AfyStream.new # The current stream segment.
        @segments = [] # The accumulated stream segments.
        @segmentsExpectOutput = false # Maybe just a workaround until commit produces bits in the response stream...
        @mode = mode # The combination of modes in which we operate currently.
        @rc = nil # The return code from Affinity.
        @pbOutput = nil # The parsed (AfyStream) protobuf output.
        @propDict = {} # Accumulated dictionary of {propname, propid}.
        @lpToken = nil # For long-running transactions (protobuf).
        @txCnt = 0 # Holds a count of nested transactions.
        @pinUpdates = [] # Accumulates PIN updates during a transaction.
      end
      # ---
      # Control of the PB stream.
      def logger() @connection.logger end
      def isOutputIgnored() (@mode & MODE_IGNORE_OUTPUT) != 0 end
      def performImmediateUpdates() (@mode & MODE_IMMEDIATE_UPDATES) != 0 end
      def capture()
        if @pbStream.pins.length > 0
          # Review: The final condition will be different, but for the moment I'm not sure I can do better.
          @segmentsExpectOutput = @pbStream.pins.map {|p| AffinityPB::AfyStream::MODOP::OP_INSERT.value==p.op ? true : nil}.compact.length > 0
          @segments << @pbStream.serialize_to_string
        elsif @pbStream.stmt.length > 0
          @segmentsExpectOutput = true
          @segments << @pbStream.serialize_to_string
        elsif @pbStream.txop.length > 0 or @pbStream.flush.length > 0 or @pbStream.properties.length > 0
          @segments << @pbStream.serialize_to_string
        else
          logger.warn("An empty pbStream was captured... and ignored.")
        end
        logger.debug("#{@segments.length} segments, #{@segments.inject(0){|sum,s| sum + s.length }} bytes #{@pbStream.flush.length > 0 ? "FLUSH" : ""}")
        @pbStream = AffinityPB::AfyStream.new
      end
      def flush(explicit=true)
        if explicit
          @pbStream.flush.push(0)
        end
        capture()
        _applyPINUpdates()
        _pushData()
      end
      # ---
      # Transaction control.
      # TODO: Offer the commit/rollback ALL option.
      # TODO: Have a block version, with auto-commit/rollback?
      def startTx(txLabel=nil)
        logger.debug("PBTransactionCtx.startTx #{txLabel}")
        if @lpToken.nil?
          @lpToken = @connection._beginlongpost()
        end
        @pbStream.txop.push(AffinityPB::AfyStream::TXOP::TX_START.value)
        capture()
        @txCnt += 1
      end
      def commitTx()
        logger.debug("PBTransactionCtx.commitTx")
        @pbStream.txop.push(AffinityPB::AfyStream::TXOP::TX_COMMIT.value)
        capture()
        @txCnt -= 1
        if 0 == @txCnt
          _terminate()
        end
      end
      def rollbackTx()
        logger.debug("PBTransactionCtx.rollbackTx")
        @pbStream.txop.push(AffinityPB::AfyStream::TXOP::TX_ROLLBACK.value)
        capture()
        @txCnt -= 1
        if 0 == @txCnt
          _terminate()
        end
      end
      def _terminate()
        logger.debug("PBTransactionCtx._terminate")
        if @txCnt > 0
          logger.warn("terminated a txctx prematurely")
        end
        _applyPINUpdates()
        _pushData()
        if !@lpToken.nil?
          @connection._endlongpost(@lpToken)
          @lpToken = nil
        end
        @propDict = {} # REVIEW: In a near future we'll try to be more efficient than this.
        @connection._txCtx_reset
      end
      # ---
      # Query via protobuf (various flavors).
      def _queryPB1(pQstr, pRtt=AffinityPB::ReturnType::RT_PINS)
        # This version really participates to the current protobuf stream and its current transaction (i.e. protobuf in&out).
        logger.info("pathSQL in protobuf: #{pQstr}")
        lStmt = AffinityPB::AfyStream::PathSQL.new
        lStmt.sq = pQstr
        lStmt.cid = @@nextCID; @@nextCID += 1
        lStmt.rtt = pRtt
        lStmt.limit = 99999 # otherwise 0 by default right now
        lStmt.offset = 0
        @pbStream.stmt.push(lStmt)
        flush()
        @pbOutput
      end
      def PBTransactionCtx._parsePBStr(pRaw)
        begin
          pb = AffinityPB::AfyStream.new
          pb.parse_from_string pRaw
        rescue => ex
          puts "Exception #{ex}: #{ex.backtrace}"
          raise
        end
      end
      def PBTransactionCtx._queryPBOut(pQstr) Connection.getCurrentDbConnection()._getProto("/db?q=#{CGI::escape(pQstr)}&i=pathsql&o=proto", nil) end
      def _queryPB2(pQstr)
        logger.info("pathSQL (in protobuf): #{pQstr}")
        return PBTransactionCtx._queryPBOut(pQstr)
      end
      alias queryPB _queryPB2 # TODO: switch to _queryPB1 when it's glitchless... (right now it's still buggier somehow).
      # ---
      # Accumulation of PIN updates.
      def recordPINUpdate(pinUpdate) # TODO: also record the original pin, to pad its ids
        # Record a PIN update (allows to defer dialogue with Affinity in some cases, and reduce chattiness).
        raise Exception.new() if performImmediateUpdates
        logger.debug("")
        @pinUpdates << pinUpdate
      end
      def _applyPINUpdates()
        # Apply all accumulated PIN updates.
        logger.debug("#{@pinUpdates.length} updates")
        if @pinUpdates.length > 0
          PIN._savePINsi(@pinUpdates, self)
          @pinUpdates = []
        end
      end
      # ---
      # Accumulation of protobuf segments.
      def _pushData()
        # Push all accumulated serialized protobuf segments to Affinity; parse and store the output.
        lSegmentsExpectOutput = @segmentsExpectOutput
        logger.debug("#{@segments.length} segments")
        lMessage = @segments.join("")
        if 0 == @txCnt
          @propDict = {} # REVIEW: In a near future we'll try to be more efficient than this.
        end
        @segments = []
        @segmentsExpectOutput = false
        @rc = nil
        @pbOutput = nil
        if 0 == lMessage.length
          logger.debug("no message to send")
          return
        end
        #logger.debug("message sent to Affinity: #{PBTransactionCtx._parsePBStr(lMessage).inspect}")
        if @lpToken
          @rc, lRawOutput = @connection._continuelongpost(@lpToken, lMessage, lSegmentsExpectOutput)
          #logger.debug("response obtained from Affinity (longpost): #{PBTransactionCtx._parsePBStr(lRawOutput).inspect}")
          if lSegmentsExpectOutput and lRawOutput
            logger.debug("result: RC=#{@rc} (#{lRawOutput.length} bytes)")
            @pbOutput = PBTransactionCtx._parsePBStr(lRawOutput)
          else
            logger.debug("result: RC=#{@rc}")
          end
        else
          @rc, lRawOutput = @connection._post(lMessage)
          #logger.debug("response obtained from Affinity: #{PBTransactionCtx._parsePBStr(lRawOutput).inspect}")
          if lSegmentsExpectOutput and lRawOutput
            logger.debug("result: RC=#{@rc} (#{lRawOutput.length} bytes)")
            @pbOutput = PBTransactionCtx._parsePBStr(lRawOutput)
          else
            logger.debug("result: RC=#{@rc}")
          end
        end
      end
      def clearPBOutput()
        @pbOutput = nil
      end
    end

    def initialize(options)
      options = options || {}
      @host = options[:host] || 'localhost'
      @port = options[:port] || 4560
      @owner = options[:owner] || 'rubytests'
      @pw = options[:pw] || nil
      logfile = options[:logfile] || './affinity.rblog'
      @logger = Logger.new(logfile, 'monthly')
      @logger.level = options[:loglevel] || Logger::WARN
      @txCtx = nil
      @connectionHttp = Net::HTTP.new(@host, @port, nil, nil, @owner, @pw) # Presently, the ruby flavor always uses keep-alive.
      @connectionHttp.start
      @logger.info("Started connection #{@connectionHttp.inspect}")
    end

    def Connection.open(*args, &block)
      lConnection = new(*args)
      Connection.pushDbConnection(lConnection)
      return lConnection unless block_given?
      yield lConnection
    ensure
      if block_given?
        Connection.popDbConnection(lConnection)
        lConnection.close
      end
    end

    def close()
      return if @connectionHttp.nil?
      @connectionHttp.finish
      @connectionHttp = nil
    end
    alias terminate close

    def keptAlive() true end # Presently, the ruby flavor always uses keep-alive.
    def q(qstr, options=nil) _getJson("/db?q=#{CGI::escape(qstr)}&i=pathsql&o=json", options) end
    def qCount(qstr) r = _getRaw("/db?q=#{CGI::escape(qstr)}&i=pathsql&o=json&type=count"); if r.nil? then 0 else r.to_i end; end
    def qProto(qstr, options=nil) if @txCtx.nil? then _getProto("/db?q=#{CGI::escape(qstr)}&i=pathsql&o=proto", options) else @txCtx.queryPB(qstr) end; end
    def createPINs(descriptions) PIN.savePINs(descriptions) end
    def startTx(txLabel=nil) _txCtx().startTx(txLabel) end
    def commitTx() _txCtx().commitTx() end
    def rollbackTx() _txCtx().rollbackTx() end
    def makeUrl(str) PIN::Url.new(str) end
    def makeRef(localpid, ident=nil, property=nil, eid=nil) PIN::Ref.new(localpid, ident, property, eid) end

    #--------
    # PRIVATE: Transaction context management.
    #--------

    def _txCtx()
      if @txCtx.nil?
        @txCtx = PBTransactionCtx.new(self)
      end
      @txCtx
    end

    def _txCtx_reset()
      @txCtx = nil
    end

    #--------
    # PRIVATE: HTTP implementation.
    #--------

    def _getJson(urlPath, options)
      begin
        r = _getRaw(urlPath, options)
        return JSON.parse(r) if (!r.nil? and r.length > 0)
      rescue => ex
        # Investigate: Currently the ruby JSON parser seems to choke on class creation results; curiously, I'm not seeing this in 'irb'; maybe a version-related issue.
        @logger.warn("Exception #{ex}: #{ex.backtrace}")
      end
      {}
    end

    def _getProto(urlPath, options)
      r = _getRaw(urlPath, options)
      return nil if r.nil?
      pb = AffinityPB::AfyStream.new
      pb.parse_from_string r
    end

    def _getRaw(urlPath, options=nil)
      raise "Connection not open" if @connectionHttp.nil? or !@connectionHttp.started?
      if options and options.has_key? :limit then urlPath += "&limit=#{options[:limit]}" end
      if options and options.has_key? :offset then urlPath += "&offset=#{options[:offset]}" end
      @logger.info("_getRaw: #{urlPath}")
      result = nil
      begin
        req = Net::HTTP::Get.new(urlPath)
        req['Authorization'] = "Basic #{Base64.encode64("#{@owner}:#{@pw}")}"
        res = @connectionHttp.request(req)
        if '200' == res.code and res.body.length > 0
          result = res.body
        else
          @logger.warn("Connection._getRaw returned nothing: http=#{res.code}")
        end
      rescue => ex
        @logger.warn("Exception #{ex}: #{ex.backtrace}")
      end
      result
    end

    def _post(msg)
      raise "Connection not open" if @connectionHttp.nil? or !@connectionHttp.started?
      @logger.info("sending protobuf message: #{msg.length} bytes")
      res = [0, nil]
      begin
        req = Net::HTTP::Post.new("/db/?i=proto&o=proto")
        req['Content-Type'] = "application/octet-stream"
        req['Authorization'] = "Basic #{Base64.encode64("#{@owner}:#{@pw}")}"
        req.body = msg
        res = @connectionHttp.request(req)
        res = [0, res.body]
      rescue => ex
        @logger.warn("Exception #{ex}: #{ex.backtrace}")
      end
      @logger.debug("received #{res[1].length} bytes")
      res
    end

    def _beginlongpost()
      # Note:
      #   For the moment, because the kernel uses an independent transaction control for protobuf, we use a parallel connection here
      #   (similar model as in python and nodejs).
      longHttp = Net::HTTP.new(@host, @port, nil, nil, @owner, @pw)
      longHttp.start
      req = Net::HTTP::Post.new("/db/?i=proto&o=proto")
      req['Content-Type'] = "application/octet-stream"
      req['Authorization'] = "Basic #{Base64.encode64("#{@owner}:#{@pw}")}"
      # From Net::HTTP.request... we dissect one HTTP request to stream in/out the protobuf segments and their responses.
      # puts "about to begin_transport..."
      # STDIN.getc
      longHttp.send(:begin_transport, req)
      sock = longHttp.instance_variable_get :@socket
      ver = longHttp.instance_variable_get :@curr_http_version
      req.send(:write_header, sock, ver, req.path)
      sock.io.fcntl(Fcntl::F_SETFL, sock.io.fcntl(Fcntl::F_GETFL) | Fcntl::O_NONBLOCK)
      [longHttp, req]
    end

    def _continuelongpost(lpToken, msg, expectOutput)
      @logger.debug("sending protobuf message");
      res = nil
      begin
        # Retrieve the raw socket handle.
        sock = lpToken[0].instance_variable_get :@socket
        io = sock.io

        # Read the basic response (HTTP_OK etc.), if pending.
        if 2 == lpToken.length
          lpToken << Net::HTTPResponse.read_new(sock)
          @logger.debug("obtained basic response: #{lpToken[2].inspect} expectOutput:#{expectOutput}")
        end

        # Send the msg segment.
        wrote = io.syswrite msg
        @logger.debug("wrote #{wrote} bytes")

        # Read the response.
        if expectOutput
          _safeioread = lambda\
          {
            |logexcpt|
            begin
              _r = io.sysread 4096
            rescue => ex
              @logger.debug("exception during io.sysread: #{ex.inspect}") if logexcpt
              sleep(0.01)
            end
            _r
          }
          while res.nil? do
            res = _safeioread.call(true)
          end
          res1 = _safeioread.call(false)
          until res1.nil? do
            res += res1
            res1 = _safeioread.call(false)
          end
        end
        @logger.warn("done reading segment: obtained #{if res.nil? then 0 else res.length end} bytes")
      rescue => ex
        @logger.warn("Exception #{ex}: #{ex.backtrace}")
      end
      [0, res]
    end

    def _endlongpost(lpToken)
      lpToken[2].instance_variable_set :@read, true if lpToken.length > 2
      lpToken[0].send(:end_transport, lpToken[1], lpToken[2])
      lpToken[0].finish
    end

    @@defaultConnection = Connection.new(nil)
  end
end
