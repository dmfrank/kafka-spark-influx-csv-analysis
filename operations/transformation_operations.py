# Copyright 2017, bwsoft management
#
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

import pyspark.sql.types as types


class Operation:
    def __init__(self, name, op_count, func):
        self.name = name
        self.op_count = op_count
        self.func = func

    def result_type(self, arg_types = []):
        raise NotImplemented("Should be implemented in concrete operation.")

    @staticmethod
    def get_larger_type(t=[]):

        avail_types = [
            types.BooleanType,
            types.ByteType,
            types.ShortType,
            types.IntegerType,
            types.LongType,
            types.FloatType,
            types.DoubleType
        ]

        if len(t) < 2:
            raise IndexError("Should be at least 2 arguments.")

        indexes = map(lambda x: avail_types.index(x), t)
        rt = avail_types[max(indexes)]
        return rt


class UnarySameTypeOperation(Operation):
    def __init__(self, name, func):
        super().__init__(name, 1, func)

    def result_type(self, arg_types):
        arg_types[0]


class Id(UnarySameTypeOperation):
    def __init__(self):
        super().__init__("id", lambda x: x)


class GreatTypeCastedOperation(Operation):
    def __init__(self, name, op_count, func):
        super().__init__(self, name, op_count, func)

    def result_type(self, arg_types):
        return self.get_larger_type(arg_types)


class MathDiv(Operation):
    def __init__(self):
        super().__init__(self, "mathdiv", 2, lambda x, y: x / float(y))

    def result_type(self, arg_types = []):
        return types.DoubleType


class Boolean(Operation):
    def result_type(self, arg_types = []):
        return types.BooleanType


class Eq(Boolean):
    def __init__(self):
        super().__init__(self,"eq", 2, lambda x,y: x == y)


class Gt(Boolean):
    def __init__(self):
        super().__init__(self,"gt", 2, lambda x,y: x > y)


class Ge(Boolean):
    def __init__(self):
        super().__init__(self,"ge", 2, lambda x,y: x >= y)


class Lt(Boolean):
    def __init__(self):
        super().__init__(self,"lt", 2, lambda x,y: x < y)


class Le(Boolean):
    def __init__(self):
        super().__init__(self,"le", 2, lambda x,y: x <= y)


class TransformationOperations:
    def add(self, operation):
        self.operations_dict[operation.name] = operation

    def __init__(self):
        self.operations_dict = {}
        self.add(Id())
        self.add(GreatTypeCastedOperation("add", lambda x, y: x + y))
        self.add(GreatTypeCastedOperation("sub", lambda x, y: x - y))
        self.add(GreatTypeCastedOperation("mul", lambda x, y: x * y))
        self.add(GreatTypeCastedOperation("odd", lambda x, y: x % y))
        self.add(GreatTypeCastedOperation("pydiv", lambda x, y: x / y))
        self.add(MathDiv())
        self.add(Eq())
        self.add(Gt())
        self.add(Ge())
        self.add(Lt())
        self.add(Le())
