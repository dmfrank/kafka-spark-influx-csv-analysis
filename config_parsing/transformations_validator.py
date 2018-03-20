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

import re

import pyspark.sql.types as types

from errors import errors
from .transformations_parser import FieldTransformation


class TransformationsValidator:
    def __init__(self, transformation_operations, data_structure_pyspark):
        self.current_fields = data_structure_pyspark
        self.transformation_operations = transformation_operations

    def __get_field(self, field):
        try:
            renamed_field = self.current_fields[field]
            return renamed_field
        except KeyError as ex:
            raise errors.FieldNotExists("Field with name {} not exists".format(field))

    def _validate_syntax_tree(self, tree):
        operation = self.transformation_operations.operations_dict.get(tree.operation, None)
        if operation is not None:
            if len(tree.children) == operation["operands"]:
                for index, ch in enumerate(tree.children):
                    actual_type = None
                    if isinstance(ch, str):  # number or field name
                        result = re.search('^(\d+)$', ch)
                        if result is not None:  # it's number
                             actual_type = types.LongType()
                        else:  # it's field
                            renamed_field = self.__get_field(ch)
                            actual_type = renamed_field.dataType
                    else:  # it's other syntax tree, we should extract operation, get type and verify
                        subtree_operation = self.transformation_operations.operations_dict.get(ch.operation, None)
                        if subtree_operation:
                            actual_type = subtree_operation["result"]
                        else:
                            raise errors.OperationNotSupportedError("Operation {} not supported".format(ch.operation))

                        self._validate_syntax_tree(ch)


                    if actual_type != operation["types"][index]:
                        raise errors.IncorrectArgumentTypeForOperationError(
                            "Operand {} expect operands with type {}, but {} has type {}".format(tree.operation,
                                                                                                 operation["types"][index],
                                                                                                 ch,
                                                                                                 actual_type))
                return operation["result"]
            else:
                raise errors.IncorrectArgumentsAmountForOperationError(
                    "Operand {} expect {} operands, but actual {}".format(tree.operation, operation["operands"],
                                                                          len(tree.children)))
        else:
            raise errors.OperationNotSupportedError("Operation {} not supported".format(tree.operation))

    def validate(self, transformations):
        new_fields = []
        for transformation in transformations:
            if isinstance(transformation, FieldTransformation):  # it's transformed name
                if isinstance(transformation.operation, str):  # it's rename
                    renamed_field = self.__get_field(transformation.operation)
                    new_fields.append(types.StructField(transformation.field_name, renamed_field.dataType))
                else:  # is Syntaxtree
                    field_type = self._validate_syntax_tree(transformation.operation)
                    new_fields.append(types.StructField(transformation.field_name, field_type))
            else:  # no transforms
                new_fields.append(self.__get_field(transformation))
        return types.StructType(new_fields)
