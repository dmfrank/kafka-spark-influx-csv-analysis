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

from pyspark.sql.types import *
import inspect

from errors import errors


class MapOperation(object):
    def __init__(self, name, op_count, func):
        self.name = name
        self.op_count = op_count
        self.func = func

    def __str__(self):
        return "{}({}) => {}".format(self.name,
                                     ",".join(map(lambda x: "arg{}".format(x), range(self.op_count))),
                                     inspect.getsource(self.func))

    def result_type(self, arg_types = []):
        raise NotImplemented("Should be implemented in concrete operation.")

    @staticmethod
    def get_larger_type(t=[]):

        avail_types = [
            BooleanType(),
            ByteType(),
            ShortType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType()
        ]

        if len(t) < 1:
            raise IndexError("Should be at least 1 argument.")

        try:
            indexes = list(map(lambda x: avail_types.index(x), t))
        except ValueError as e:
            raise errors.IncorrectArgumentTypeForOperationError(e)
        rt = avail_types[max(indexes)]
        return rt


class UnarySameTypeOperation(MapOperation):
    def __init__(self, name, func):
        super().__init__(name, 1, func)

    def result_type(self, arg_types):
        arg_types[0]


class Id(UnarySameTypeOperation):
    def __init__(self):
        super().__init__("id", lambda x: x)


class GreatTypeCastedOperation(MapOperation):
    def __init__(self, name, op_count, func):
        super().__init__(name, op_count, func)

    def result_type(self, arg_types):
        return self.get_larger_type(arg_types)


class MathDiv(MapOperation):
    def __init__(self):
        super().__init__("mathdiv", 2, lambda x, y: x / float(y))

    def result_type(self, arg_types = []):
        return DoubleType()


class Boolean(MapOperation):
    def result_type(self, arg_types = []):
        return BooleanType()


class Cast(MapOperation):
    def __init__(self,name, new_type, function):
        super().__init__(name, 1, function)
        self.ret_type = new_type

    def result_type(self, arg_types = []):
        return self.ret_type


class Truncate(MapOperation):
    def __init__(self):
        super().__init__("truncate", 2, lambda x, length: x[:length] )

    def result_type(self, arg_types = []):
        if len(arg_types) != 2:
            raise ValueError("Truncate expects 2 arguments. Got '{}'".format(len(arg_types)))

        if arg_types[0] != StringType():
            raise errors.IncorrectArgumentTypeForOperationError("First argument of Truncate should be a string. Got {}".format(arg_types[0]))

        if not (arg_types[1] == IntegerType() or arg_types[1] == LongType()):
            raise errors.IncorrectArgumentTypeForOperationError("Second argument should be a long or int. Got {}".format(arg_types[1]))

        return StringType()


class TransformationOperations:
    def add(self, operation):
        self.operations_dict[operation.name] = operation

    def __init__(self):
        self.operations_dict = {}
        self.add(Id())

        self.add(GreatTypeCastedOperation("add", 2, lambda x, y: x + y))
        self.add(GreatTypeCastedOperation("sub", 2, lambda x, y: x - y))
        self.add(GreatTypeCastedOperation("mul", 2, lambda x, y: x * y))
        self.add(GreatTypeCastedOperation("odd", 2, lambda x, y: x % y))
        self.add(GreatTypeCastedOperation("pydiv", 2, lambda x, y: x / y))
        self.add(MathDiv())

        self.add(Boolean("lt", 2, lambda x,y: x < y))
        self.add(Boolean("le", 2, lambda x,y: x <= y))
        self.add(Boolean("gt", 2, lambda x,y: x > y))
        self.add(Boolean("ge", 2, lambda x,y: x >= y))
        self.add(Boolean("eq", 2, lambda x,y: x == y))
        self.add(Boolean("neq", 2, lambda x,y: x != y))
        self.add(Boolean("or", 2, lambda x,y: x or y))
        self.add(Boolean("and", 2, lambda x,y: x and y))

        self.add(Cast("long",LongType(), lambda x: int(x)))
        self.add(Cast("int", IntegerType(), lambda x: int(x)))
        self.add(Cast("float", FloatType(), lambda x: float(x)))
        self.add(Cast("double", DoubleType(), lambda x: float(x)))
        self.add(Cast("boolean", BooleanType(), lambda x: bool(x)))
        self.add(Cast("not", BooleanType(), lambda x: not x))

        self.add(Cast("one", IntegerType(), lambda x: 1))
        self.add(Truncate())
