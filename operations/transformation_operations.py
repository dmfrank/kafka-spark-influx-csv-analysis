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

class TransformationOperations:
    def __init__(self):
        self.reference = {
            "seal.t1" : {
                "t_low"   : -100,
                "t_hi"    : +500,
                "rpm_low" : 1000,
                "rpm_hi"  : 4000,
                "p_low"   : 200,
                "p_hi"    : 1500
            },
            "seal.t2" : {
                "t_low"   : -50,
                "t_hi"    : +700,
                "rpm_low" : 1000,
                "rpm_hi"  : 4000,
                "p_low"   : 700,
                "p_hi"    : 3500
            }
        }

        self.operations_dict = {
            "add": {
                "operands": 2,
                "types": [types.DoubleType(), types.DoubleType()],
                "result": types.DoubleType(),
                "lambda": lambda x, y: x + y
            },
            "sub": {
                "operands": 2,
                "types": [types.DoubleType(), types.DoubleType()],
                "result": types.DoubleType(),
                "lambda": lambda x, y: x - y
            },
            "mul": {
                "operands": 2,
                "types": [types.DoubleType(), types.DoubleType()],
                "result": types.DoubleType(),
                "lambda": lambda x, y: x * y
            },
            "div": {
                "operands": 2,
                "types": [types.DoubleType(), types.DoubleType()],
                "result": types.DoubleType(),
                "lambda": lambda x, y: x / y
            },
            "lt": {
                "operands": 2,
                "types": [types.DoubleType(), types.DoubleType()],
                "result": types.LongType(),
                "lambda": lambda x, y: 1 if x < y else 0
            },
            "le": {
                "operands": 2,
                "types": [types.DoubleType(), types.DoubleType()],
                "result": types.LongType(),
                "lambda": lambda x, y: 1 if x <= y else 0
            },
            "gt": {
                "operands": 2,
                "types": [types.DoubleType(), types.DoubleType()],
                "result": types.LongType(),
                "lambda": lambda x, y: 1 if x > y else 0
            },
            "ge": {
                "operands": 2,
                "types": [types.DoubleType(), types.DoubleType()],
                "result": types.LongType(),
                "lambda": lambda x, y: 1 if x >= y else 0
            },
            "eq": {
                "operands": 2,
                "types": [types.DoubleType(), types.DoubleType()],
                "result": types.LongType(),
                "lambda": lambda x, y: 1 if abs(x - y) < 0.0001 else 0
            },
            "count": {
                "operands": 1,
		"types": [types.LongType()],
                "result": types.LongType(),
                "lambda": lambda x: 1
            },
            "id": {
                "operands": 1,
                "types": [types.DoubleType()],
                "result": types.DoubleType(),
                "lambda": lambda x: x
            },
            "reference_rpm_hi": {
                "operands": 1,
                "types": [types.StringType()],
                "result": types.DoubleType(),
                "lambda": lambda dt: self.reference[dt]['rpm_hi']
            },
            "reference_rpm_low": {
                "operands": 1,
                "types": [types.StringType()],
                "result": types.DoubleType(),
                "lambda": lambda dt: self.reference[dt]['rpm_low']
            },
            "round": {
                "operands": 2,
                "types": [types.DoubleType(), types.IntegerType()],
                "result": types.FloatType(),
                "lambda": lambda num, digits: round(num, digits)
            },
            "truncate": {
                "operands": 2,
                "types": [types.StringType(), types.LongType()],
                "result": types.StringType(),
                "lambda": lambda long_string,length: long_string[:length]
            }
        }
