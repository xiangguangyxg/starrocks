// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.OperatorType.ARRAY;

/**
 * ArrayOperator corresponds to ArrayExpr at the syntax level.
 * When Array is explicitly declared, ArrayExpr will be generated
 * eg. array[1,2,3]
 */
public class ArrayOperator extends ArgsScalarOperator {
    private final boolean nullable;

    public ArrayOperator(Type type, boolean nullable, List<ScalarOperator> arguments) {
        super(ARRAY, type);
        this.arguments = arguments;
        this.nullable = nullable;
        incrDepth(arguments);
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return arguments.stream().map(ScalarOperator::toString).collect(Collectors.joining(","));
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet usedColumns = new ColumnRefSet();
        arguments.forEach(arg -> usedColumns.union(arg.getUsedColumns()));
        return usedColumns;
    }

    @Override
    public ScalarOperator clone() {
        ArrayOperator operator = (ArrayOperator) super.clone();
        // Deep copy here
        List<ScalarOperator> newArguments = Lists.newArrayList();
        this.arguments.forEach(p -> newArguments.add(p.clone()));
        operator.arguments = newArguments;
        return operator;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitArray(this, context);
    }

    @Override
    public boolean equalsSelf(Object o) {
        if (!super.equalsSelf(o)) {
            return false;
        }
        ArrayOperator that = (ArrayOperator) o;
        return nullable == that.nullable;
    }

    @Override
    public int hashCodeSelf() {
        return Objects.hash(type);
    }
}
