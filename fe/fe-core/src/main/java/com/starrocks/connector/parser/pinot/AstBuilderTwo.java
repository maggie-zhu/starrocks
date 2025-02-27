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
package com.starrocks.connector.parser.pinot;

import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.parser.SyntaxSugars;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AstBuilderTwo implements SqlVisitor<ParseNode> {

    private final long sqlMode;
    public AstBuilderTwo(long sqlMode) {
        this.sqlMode = sqlMode;
    }

    @Override
    public ParseNode visit(SqlLiteral sqlLiteral) {
        return null;
    }

    @Override
    public ParseNode visit(SqlCall sqlCall) {
        if (sqlCall instanceof SqlBasicCall) {
            return visitSqlBasic((SqlBasicCall) sqlCall);
        } else {
            switch (sqlCall.getKind()) {
                case SELECT:
                    return visitSelect((SqlSelect) sqlCall);
                case IDENTIFIER:
                    return sqlCall.accept(this);
                default:
                    throw new UnsupportedOperationException("Unsupported SQL call: " + sqlCall);
            }
        }
    }

    @Override
    public ParseNode visit(SqlNodeList sqlNodeList) {
        return null;
    }

    @Override
    public ParseNode visit(SqlIdentifier sqlIdentifier) {
        return null;
    }

    @Override
    public ParseNode visit(SqlDataTypeSpec sqlDataTypeSpec) {
        return null;
    }

    @Override
    public ParseNode visit(SqlDynamicParam sqlDynamicParam) {
        return null;
    }

    @Override
    public ParseNode visit(SqlIntervalQualifier sqlIntervalQualifier) {
        return null;
    }

    private ParseNode visitSelect(SqlSelect select) {
        // 1. parse SELECT list
        List<SelectListItem> selectItems = select.getSelectList().getList().stream()
                .map(selectItem -> visit((SqlBasicCall) selectItem)).map(selectItem -> (SelectListItem) selectItem)
                .collect(Collectors.toList());

        boolean isDistinct = select.isDistinct();
        SelectList selectList = new SelectList(selectItems, isDistinct);

        // 2. parse FROM clause
        Relation fromNode = select.getFrom() != null ? (Relation) select.getFrom().accept(this) : null;

        // 3. parse WHERE clause（might be empty）
        ParseNode whereNode = select.getWhere() != null ? select.getWhere().accept(this) : null;

        // 4. parse GROUP BY clause（might be empty）
        List<ParseNode> groupByNodes = select.getGroup() != null
                ? select.getGroup().getList().stream().map(node -> node.accept(this)).collect(Collectors.toList())
                : Collections.emptyList();

        // 5. parse HAVING clause（might be empty）
        ParseNode havingNode = select.getHaving() != null ? select.getHaving().accept(this) : null;

        // 6. parse ORDER BY clause（might be empty）
        List<OrderByElement> orderByNodes = select.getOrderList() != null
                ? select.getOrderList().getList().stream()
                .map(node -> node.accept(this)).map(orderByItem -> (OrderByElement) orderByItem)
                .collect(Collectors.toList())
                : Collections.emptyList();

        // 7. parse LIMIT / OFFSET
        ParseNode offsetNode = select.getOffset() != null ? select.getOffset().accept(this) : null;

        SelectRelation resultSelectRelation = new SelectRelation(
                selectList,
                fromNode,
                (Expr) whereNode,
                (GroupByClause) groupByNodes,
                (Expr) havingNode);

        if (select.hasOrderBy()) {
            resultSelectRelation.setOrderBy(orderByNodes);
        } else {
            resultSelectRelation.setOrderBy(new ArrayList<>());
        }

        if (offsetNode != null) {
            LimitElement limitElement = (LimitElement) offsetNode;
            resultSelectRelation.setLimit(limitElement);
        }

        return resultSelectRelation;
    }

    private ParseNode visitSqlBasic(SqlBasicCall sqlBasicCall) {
        SqlOperator operator = sqlBasicCall.getOperator();

        //deal with functions
        if (operator instanceof SqlFunction) {
            return visitFunction(sqlBasicCall);
        }

        // 3️⃣ 处理算术运算 (+, -, *, /)
        if (operator instanceof SqlBinaryOperator && operator.getKind().belongsTo(SqlKind.BINARY_ARITHMETIC)) {
            if (sqlBasicCall.operandCount() == 2) {  // 确保是二元运算符
                ParseNode left = sqlBasicCall.operand(0).accept(this);
                ParseNode right = sqlBasicCall.operand(1).accept(this);
                return new ArithmeticExpr(ArithmeticExpr.Operator.ADD, (Expr) left, (Expr) right);
            }
        }

        // 4️⃣ 处理逻辑运算 (AND, OR, NOT)
        if (operator instanceof SqlBinaryOperator && (operator.getKind() == SqlKind.AND || operator.getKind() == SqlKind.OR)) {
            ParseNode left = sqlBasicCall.operand(0).accept(this);
            ParseNode right = sqlBasicCall.operand(1).accept(this);
            return new CompoundPredicate(CompoundPredicate.Operator.AND, (Expr) left, (Expr) right);
        }

        // 5️⃣ 处理比较运算 (=, !=, >, <, >=, <=)
        if (operator instanceof SqlBinaryOperator && operator.getKind().belongsTo(SqlKind.COMPARISON)) {
            ParseNode left = sqlBasicCall.operand(0).accept(this);
            ParseNode right = sqlBasicCall.operand(1).accept(this);
            return new BinaryPredicate(BinaryType.GE, (Expr) left, (Expr) right);
        }

        throw new UnsupportedOperationException("Unsupported SqlBasicCall operator: " + sqlBasicCall.getOperator());
    }

    private ParseNode visitFunction(SqlBasicCall sqlBasicCall) {
        List<Expr> arguments = sqlBasicCall.getOperandList().stream()
                .map(operand -> (Expr) operand.accept(this))
                .collect(Collectors.toList());

        Expr callExpr;
        String functionName = sqlBasicCall.getOperator().getName();
        Pinot2SRFunctionCallTransformer transformer = new Pinot2SRFunctionCallTransformer();
        Expr convertedFunctionCall = transformer.convert(functionName, arguments);

        if (convertedFunctionCall != null) {
            if (functionName.equalsIgnoreCase("fromdatetime")) {
                ArithmeticExpr toMillis = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY,
                        convertedFunctionCall, new IntLiteral(1000));
                return toMillis;
            }
            return convertedFunctionCall;
        } else {
            FunctionCallExpr functionCallExpr = new FunctionCallExpr(functionName,
                    new FunctionParams(false, arguments));
            return SyntaxSugars.parse(functionCallExpr);
        }
    }

}
