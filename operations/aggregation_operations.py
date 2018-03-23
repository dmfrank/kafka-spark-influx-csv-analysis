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


class ReduceOperation:
    def __init__(self, name, function, supported_arg_types=[]):
        self.name = name
        self.function = function
        self.supported_arg_types = supported_arg_types

    def output_type(self, input_type):
        return input_type

    @classmethod
    def std_scalar_math_types(cls):
        return [BooleanType(),
                ByteType(),
                ShortType(),
                IntegerType(),
                LongType(),
                FloatType(),
                DoubleType()]

    def is_type_compatible(self, input_type):
        try:
            self.supported_arg_types.index(input_type)
            return True
        except ValueError:
            return False


class SupportedReduceOperations:
    def add(self, operation):
        self.operation[operation.name] = operation

    def is_type_compatible(self, input_type, function_name):
        return self.operation[function_name].is_type_compatible(input_type)

    def __init__(self):
        self.operation = {}

        self.add(ReduceOperation("sum", lambda x, y: x + y, ReduceOperation.std_scalar_math_types()))
        self.add(ReduceOperation("mul", lambda x, y: x * y, ReduceOperation.std_scalar_math_types()))
        self.add(ReduceOperation("max", lambda x, y: y if x < y else x, ReduceOperation.std_scalar_math_types()))
        self.add(ReduceOperation("min", lambda x, y: y if x > y else x, ReduceOperation.std_scalar_math_types()))
