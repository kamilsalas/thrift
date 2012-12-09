#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys, glob
from optparse import OptionParser
parser = OptionParser()
parser.add_option('--genpydir', type='string', dest='genpydir', default='gen-py')
options, args = parser.parse_args()
del sys.argv[1:] # clean up hack so unittest doesn't complain
sys.path.insert(0, options.genpydir)
sys.path.insert(0, glob.glob('../../lib/py/build/lib.*')[0])

from ThriftTest.ttypes import *
from DebugProtoTest.ttypes import *
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TProtocol, TBinaryProtocol, TCompactProtocol
from thrift.TSerialization import serialize, deserialize
import unittest
import time

class ValidationTest(unittest.TestCase):

  def setUp(self):
    self.one = OneOfEach()
    self.enum = StructWithSomeEnum()
    self.struct_a = StructA(s="str")
    self.struct_b = StructB(ab = self.struct_a)
    self.struct_a_wrong = StructA()
    self.bonk = Bonk()
    self.nesting = Nesting(my_bonk = self.bonk, my_ooe = self.one)
    self.deltas = LargeDeltas()
    self.guess = GuessProtocolStruct()
    self.somemap = StructWithASomemap(somemap_field = {1:1, 2:2, 3:3})
    self.nested  = NestedMixedx2()

  def _test(self, variable_struct, variable_name, incorrect_value, 
      correct_value, validate_struct = None):
    if validate_struct is None:
      validate_struct = variable_struct
    validate_struct.validate()
    setattr(variable_struct, variable_name, incorrect_value)
    try:
      validate_struct.validate()
      self.fail("Struct should be in invalid state.")
    except TProtocol.TProtocolException:
      pass
    setattr(variable_struct, variable_name, correct_value)
    validate_struct.validate()

  def testBoolWrongType(self):
    self._test(self.one, "im_true", "String", True)

  def testByteWrongType(self):
    self._test(self.one, "a_bite", 3.14, 1)

  def testByteValueOutOfBound(self):
    self._test(self.one, "a_bite", 128, -1)
    self._test(self.one, "a_bite", -129, 1)

  def testI16WrongType(self):
    self._test(self.one, "integer16", "str", 1)

  def testI16ValueOutOfBound(self):
    self._test(self.one, "integer16", 32768, 128)
    self._test(self.one, "integer16", -32769, -129)

  def testI32WrongType(self):
    self._test(self.one, "integer32", "str", 64000)

  def testI64WrongType(self):
    self._test(self.one, "integer64", 1.6, 8589934599)

  def testFloatWrongType(self):
    self._test(self.one, "double_precision", "str", 3.14)

  def testStrWrongType(self):
    self._test(self.one, "some_characters", True, "str")

  def testEnumWrongType(self):
    self._test(self.enum, "blah", "str", SomeEnum.ONE)

  def testEnumOutOfBound(self):
    self._test(self.enum, "blah", 3, SomeEnum.TWO)
    self._test(self.enum, "blah", -1, SomeEnum.ONE)

  def testRequiredValue(self):
    self._test(self.struct_a, "s", None, "str")

  def testNestedRequiredValue(self):
    self._test(self.struct_a, "s", None, "str", self.struct_b)
    self._test(self.struct_b, "aa", self.struct_a_wrong, None, self.struct_b)

  def testNestedStructures(self):
    self._test(self.nesting, "my_bonk", "str", self.bonk)
    self._test(self.bonk, "type", "str", 123, self.nesting)
    self._test(self.one, "im_false", 3.14, False, self.nesting)

  def testListWrongType(self):
    self._test(self.one, "byte_list", 123, [1,2,3])

  def testListWrongElementType(self):
    self._test(self.one, "byte_list", [1, 2, 3, 123123], [1,2,3])
    self._test(self.one, "i16_list", [1.1, 1.2, 1.3], [1,2,3])

  def testSetWrongType(self):
    self._test(self.deltas, "a_set2500", "str", set(["a","b","c"]))

  def testSetWrongElementType(self):
    self._test(self.deltas, "a_set2500", set(["a", "b", "c", 1.0]), set(["a"]))
    self._test(self.deltas, "a_set2500", set([True, False]), set(["a", "b", "c"]))

  def testMapWrongType(self):
    self._test(self.guess, "map_field", True, dict())

  def testMapWrongKeyType(self):
    self._test(self.guess, "map_field", {"a" : "b", 1 : "str"}, {"a" : "b"}) 
    self._test(self.guess, "map_field", {"a" : "b", 1.3 : "haha"} , {"a" : "b"})

  def testMapWrongElementType(self):
    self._test(self.guess, "map_field", {"a" : "b", "b" : 1}, {"a" : "b"})
    self._test(self.guess, "map_field", {"a" : "c", "d" : True}, {"a" : "b"})

  def testTypedef(self):
    self._test(self.somemap, "somemap_field", "hahaha", {1:1, 2:2, 3:3})

  def testBinaryWrongType(self):
    self._test(self.one, "base64", 123, "base")

  def testNestedContainers(self):
    set_i32_a = set([1, 2, 3])
    set_i32_b = set([4, 5, 6])
    set_i32_c = set([7, 8, 9])
    set_string_a = set(["a", "b", "c"])
    set_string_b = set(["d", "e", "f"])
    set_string_c = set(["g", "h", "j"])
    set_string_d = set(["z", "x", "y"])
    set_string_e = set(["h", "i", "j"])
    map_a = {1:set_string_a, 2:set_string_b}
    map_b = {3:set_string_c, 4:set_string_d}
    map_c = {1:set_string_d, 8:set_string_e}
    self.nested.int_set_list = [set_i32_a, set_i32_b, set_i32_c]
    self.nested.map_int_strset = {1:set_string_a, 
                                  2:set_string_b, 
                                  3:set_string_c}
    self.nested.map_int_strset_list = [map_a, map_b, map_c]

    self.nested.validate()
    set_i32_a.add("str")
    try:
      self.nested.validate()
      self.fail("Struct should be in invalid state.")
    except TProtocol.TProtocolException:
      pass
    set_string_e.add(True)
    try:
      self.nested.validate()
      self.fail("Struct should be in invalid state.")
    except TProtocol.TProtocolException:
      pass
    set_i32_a.remove("str")
    try:
      self.nested.validate()
      self.fail("Struct should be in invalid state.")
    except TProtocol.TProtocolException:
      pass
    set_string_e.remove(True)
    self.nested.validate()
    map_a["merry christmas"] = set_string_c
    try:
      self.nested.validate()
      self.fail("Struct should be in invalid state.")
    except TProtocol.TProtocolException:
      pass
    del map_a["merry christmas"]
    self.nested.validate()


def suite():
  suite = unittest.TestSuite()
  loader = unittest.TestLoader()

  suite.addTest(loader.loadTestsFromTestCase(ValidationTest))
  return suite

if __name__ == "__main__":
  unittest.main(defaultTest="suite", testRunner=unittest.TextTestRunner(verbosity=2))
