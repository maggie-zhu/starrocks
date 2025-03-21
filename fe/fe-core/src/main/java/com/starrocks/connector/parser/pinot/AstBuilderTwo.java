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
import com.starrocks.analysis.BetweenPredicate;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TypeDef;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.*;
import com.starrocks.sql.parser.SyntaxSugars;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlVisitor;

import java.math.BigDecimal;
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
        switch (sqlLiteral.getTypeName()) {
            case DECIMAL:
            case INTEGER:
                return new IntLiteral(((BigDecimal) sqlLiteral.getValue()).longValue());
            case DOUBLE:
                try {
                    return new FloatLiteral(((BigDecimal) sqlLiteral.getValue()).doubleValue());
                } catch (AnalysisException e) {
                    throw new RuntimeException(e);
                }
            case CHAR:
                return new StringLiteral(sqlLiteral.getValueAs(String.class));
            case BOOLEAN:
                return new BoolLiteral(sqlLiteral.getValueAs(Boolean.class));
            case NULL:
                return new NullLiteral();
            case TIME:
            case TIMESTAMP:
            case DATE:
                try {
                    return new DateLiteral(sqlLiteral.getValueAs(String.class), Type.DATE);
                } catch (AnalysisException e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new UnsupportedOperationException("Unsupported literal type: " + sqlLiteral.getTypeName());
        }
    }

    @Override
    public ParseNode visit(SqlCall sqlCall) {
        if (sqlCall instanceof SqlBasicCall) {
            try {
                return visitSqlBasic((SqlBasicCall) sqlCall);
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        } else {
            switch (sqlCall.getKind()) {
                case SELECT:
                    return visitSelect((SqlSelect) sqlCall);
                case IDENTIFIER:
                    return sqlCall.accept(this);
                case ORDER_BY:
                    return visitOrderBy((SqlOrderBy) sqlCall);
                case AS:
                    return visitAs((SqlCall) sqlCall);
                case UNION:
                case INTERSECT:
                case EXCEPT:
                    return visitSetOperation((SqlCall) sqlCall);
                case JOIN:
                    return visitJoin((SqlJoin) sqlCall);
                case CASE:
                    return visitCase((SqlCase) sqlCall);
                default:
                    throw new UnsupportedOperationException("Unsupported SQL call: " + sqlCall);
            }
        }
    }

    private ParseNode visitJoin(SqlJoin sqlJoin) {
        Relation left = (Relation) sqlJoin.getLeft().accept(this);
        Relation right = (Relation) sqlJoin.getRight().accept(this);

        JoinOperator joinType;
        switch (sqlJoin.getJoinType()) {
            case INNER:
                joinType = JoinOperator.INNER_JOIN;
                break;
            case LEFT:
                joinType = JoinOperator.LEFT_OUTER_JOIN;
                break;
            case RIGHT:
                joinType = JoinOperator.RIGHT_OUTER_JOIN;
                break;
            case FULL:
                joinType = JoinOperator.FULL_OUTER_JOIN;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + sqlJoin.getJoinType());
        }

        Expr condition = null;
        if (sqlJoin.getCondition() != null) {
            condition = (Expr) sqlJoin.getCondition().accept(this);
        }

        return new JoinRelation(joinType ,left, right, condition, false);
    }

    private ParseNode visitCase(SqlCase sqlCase) {
        Expr caseExpr = null;
        if (sqlCase.getValueOperand() != null) {
            caseExpr = (Expr) sqlCase.getValueOperand().accept(this);
        }

        List<CaseWhenClause> whenClauses = new ArrayList<>();
        for (int i = 0; i < sqlCase.getWhenOperands().size(); i++) {
            Expr whenExpr = (Expr) sqlCase.getWhenOperands().get(i).accept(this);
            Expr thenExpr = (Expr) sqlCase.getThenOperands().get(i).accept(this);
            whenClauses.add(new CaseWhenClause(whenExpr, thenExpr));
        }

        Expr elseExpr = null;
        if (sqlCase.getElseOperand() != null) {
            elseExpr = (Expr) sqlCase.getElseOperand().accept(this);
        }

        return new CaseExpr(caseExpr, whenClauses, elseExpr);
    }

    private ParseNode visitSetOperation(SqlCall sqlCall) {
        List<QueryRelation> relations = sqlCall.getOperandList().stream()
                .map(node -> (QueryRelation) node.accept(this))
                .collect(Collectors.toList());

        boolean isAll = sqlCall.operand(2) != null && sqlCall.operand(2).toString().equalsIgnoreCase("ALL");

        switch (sqlCall.getKind()) {
            case UNION:
                return new UnionRelation(relations, isAll ? SetQualifier.ALL : SetQualifier.DISTINCT);
            case INTERSECT:
                return new IntersectRelation(relations, isAll ? SetQualifier.ALL : SetQualifier.DISTINCT);
            case EXCEPT:
                return new ExceptRelation(relations, isAll ? SetQualifier.ALL : SetQualifier.DISTINCT);
            default:
                throw new UnsupportedOperationException("Unsupported set operation: " + sqlCall.getKind());
        }
    }

    private ParseNode visitAs(SqlCall sqlCall) {
        ParseNode expr = sqlCall.operand(0).accept(this);
        String alias = sqlCall.operand(1).toString();

        if (expr instanceof Expr) {
            return  new SelectListItem((Expr) expr, alias);
        } else if (expr instanceof Relation) {
            return new SubqueryRelation((QueryStatement) expr);
        }

        throw new UnsupportedOperationException("Unsupported AS expression: " + expr.getClass().getName());
    }

    private ParseNode visitOrderBy(SqlOrderBy sqlOrderBy) {
        ParseNode query = sqlOrderBy.query.accept(this);

        List<OrderByElement> orderByElements = new ArrayList<>();
        for (SqlNode orderItem : sqlOrderBy.orderList) {
            OrderByElement element = (OrderByElement) orderItem.accept(this);
            orderByElements.add(element);
        }

        if (query instanceof SelectRelation) {
            SelectRelation selectRelation = (SelectRelation) query;
            selectRelation.setOrderBy(orderByElements);

            if (sqlOrderBy.fetch != null) {
                long limit = ((IntLiteral) sqlOrderBy.fetch.accept(this)).getValue();
                long offset = 0;
                if (sqlOrderBy.offset != null) {
                    offset = ((IntLiteral) sqlOrderBy.offset.accept(this)).getValue();
                }
                selectRelation.setLimit(new LimitElement(offset, limit));
            }

            return selectRelation;
        }

        throw new UnsupportedOperationException("ORDER BY can only be applied to SELECT statements");
    }

    @Override
    public ParseNode visit(SqlNodeList sqlNodeList) {
        List<ParseNode> nodes = new ArrayList<>();
        for (SqlNode node : sqlNodeList) {
            nodes.add(node.accept(this));
        }

        return nodes.isEmpty() ? null : nodes.get(0);
    }

    @Override
    public ParseNode visit(SqlIdentifier sqlIdentifier) {
        if (sqlIdentifier.isStar()) {
            // 处理 * 通配符
            return new SlotRef(QualifiedName.of(Collections.singletonList("*")));
        }

        List<String> names = sqlIdentifier.names;
        QualifiedName qualifiedName = QualifiedName.of(names);

        return new SlotRef(qualifiedName);

        // SlotRef的构造函数会根据QualifiedName中parts的数量自动处理:
        // - 如果parts.size() == 1，将被视为列名
        // - 如果parts.size() == 2，将被视为 table.column
        // - 如果parts.size() == 3，将被视为 database.table.column
        // - 如果parts.size() == 4，将被视为 catalog.database.table.column
    }

    @Override
    public ParseNode visit(SqlDataTypeSpec sqlDataTypeSpec) {
        // 将Pinot的数据类型转换为StarRocks的数据类型
        String typeName = sqlDataTypeSpec.getTypeName().getSimple();
        int length = -1;

        Type typeDef;
        switch (typeName.toUpperCase()) {
            case "VARCHAR":
            case "STRING":
                typeDef = ScalarType.createVarcharType(length);
                break;
            case "CHAR":
                typeDef = ScalarType.createCharType(length);
                break;
            case "TINYINT":
                typeDef = ScalarType.createType(PrimitiveType.TINYINT);
                break;
            case "SMALLINT":
                typeDef = ScalarType.createType(PrimitiveType.SMALLINT);
                break;
            case "INT":
            case "INTEGER":
                typeDef = ScalarType.createType(PrimitiveType.INT);
                break;
            case "BIGINT":
                typeDef = ScalarType.createType(PrimitiveType.BIGINT);
                break;
            case "FLOAT":
                typeDef = ScalarType.createType(PrimitiveType.FLOAT);
                break;
            case "DOUBLE":
                typeDef = ScalarType.createType(PrimitiveType.DOUBLE);
                break;
            case "BOOLEAN":
                typeDef = ScalarType.createType(PrimitiveType.BOOLEAN);
                break;
            case "TIMESTAMP":
            case "DATE":
                typeDef = ScalarType.createType(PrimitiveType.DATETIME);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + typeName);
        }

        return (ParseNode) typeDef;
    }

    @Override
    public ParseNode visit(SqlDynamicParam sqlDynamicParam) {
        // 处理参数绑定，如 WHERE col = ?
//        return new ParameterExpr(sqlDynamicParam.getIndex());
        return null;
    }

    @Override
    public ParseNode visit(SqlIntervalQualifier sqlIntervalQualifier) {
        // 处理时间间隔，如 INTERVAL '1' DAY
//        return new IntervalLiteral(
//                sqlIntervalQualifier.get,
//                sqlIntervalQualifier.getStartUnit().toString()
//        );
        return null;
    }

    private ParseNode visitSelect(SqlSelect select) {
        // 1. parse SELECT list
        List<SelectListItem> selectItems = new ArrayList<>();
        for (SqlNode item : select.getSelectList()) {
            if (item instanceof SqlIdentifier && ((SqlIdentifier) item).isStar()) {
                // 处理 * 号
                SlotRef starRef = new SlotRef(QualifiedName.of(Collections.singletonList("*")));
                selectItems.add(new SelectListItem(starRef, null));
            }  else if (item instanceof SqlBasicCall && ((SqlBasicCall) item).getOperator().getKind() == SqlKind.AS) {
                // 直接处理AS表达式
                SqlBasicCall asCall = (SqlBasicCall) item;
                Expr expr = (Expr) asCall.operand(0).accept(this);
                String alias = asCall.operand(1).toString();
                selectItems.add(new SelectListItem(expr, alias));
            } else {
                ParseNode parsedItem = item.accept(this);
                if (parsedItem instanceof SelectListItem) {
                    selectItems.add((SelectListItem) parsedItem);
                } else if (parsedItem instanceof Expr) {
                    selectItems.add(new SelectListItem((Expr) parsedItem, null));
                } else {
                    throw new UnsupportedOperationException("Unsupported select item: " + parsedItem);
                }
            }
        }

        boolean isDistinct = select.isDistinct();
        SelectList selectList = new SelectList(selectItems, isDistinct);

        // 2. parse FROM clause
        Relation fromNode = select.getFrom() != null ? (Relation) select.getFrom().accept(this) : null;

        // 3. parse WHERE clause
        Expr whereNode = select.getWhere() != null ? (Expr) select.getWhere().accept(this) : null;

        // 4. parse GROUP BY clause
        GroupByClause groupByClause = null;
        if (select.getGroup() != null && !select.getGroup().getList().isEmpty()) {
            List<Expr> groupByExprs = select.getGroup().getList().stream()
                    .map(node -> (Expr) node.accept(this))
                    .collect(Collectors.toList());
            groupByClause = new GroupByClause((ArrayList<Expr>) groupByExprs, GroupByClause.GroupingType.GROUP_BY);
        }

        // 5. parse HAVING clause
        Expr havingNode = select.getHaving() != null ? (Expr) select.getHaving().accept(this) : null;

        // 6. parse ORDER BY clause
        List<OrderByElement> orderByNodes = new ArrayList<>();
        if (select.getOrderList() != null) {
            for (SqlNode node : select.getOrderList()) {
                if (node instanceof SqlBasicCall) {
                    SqlBasicCall call = (SqlBasicCall) node;
                    Expr expr = (Expr) call.operand(0).accept(this);
                    boolean isAsc = !(call.getOperator().getKind() == SqlKind.DESCENDING);
                    Boolean nullFirst = call.getOperator().getKind() == SqlKind.NULLS_FIRST;
                    orderByNodes.add(new OrderByElement(expr, isAsc, nullFirst));
                } else {
                    Expr expr = (Expr) node.accept(this);
                    orderByNodes.add(new OrderByElement(expr, true, true)); // Default to ASC
                }
            }
        }

        // 7. parse LIMIT / OFFSET
        LimitElement limitElement = null;
        if (select.getFetch() != null) {
            limitElement = (LimitElement) select.getFetch().accept(this);
        }

        SelectRelation resultSelectRelation = new SelectRelation(
                selectList,
                fromNode,
                whereNode,
                groupByClause,
                havingNode);

        if (!orderByNodes.isEmpty()) {
            resultSelectRelation.setOrderBy(orderByNodes);
        } else {
            resultSelectRelation.setOrderBy(new ArrayList<>());
        }

        if (limitElement != null) {
            resultSelectRelation.setLimit(limitElement);
        }

        return resultSelectRelation;
    }

    private ParseNode visitSqlBasic(SqlBasicCall sqlBasicCall) throws AnalysisException {
        SqlOperator operator = sqlBasicCall.getOperator();

        // 处理函数调用
        if (operator instanceof SqlFunction) {
            return visitFunction(sqlBasicCall);
        }

        // 处理别名 (AS)
        if (operator.getKind() == SqlKind.AS) {
            ParseNode expr = sqlBasicCall.operand(0).accept(this);
            String alias = sqlBasicCall.operand(1).toString();

            if (expr instanceof Expr) {
                return expr;
            }
//            else if (expr instanceof Relation) {
//                return new SubqueryRelation(alias, (Relation) expr);
//            }
        }

        // 处理星号表达式 (*)
        if (operator.getKind() == SqlKind.IS_DISTINCT_FROM) {
            return new SlotRef(QualifiedName.of(Collections.singletonList("*")));
        }

//        // 处理排序表达式 (ASC/DESC)
//        if (operator.getKind() == SqlKind.ASC || operator.getKind() == SqlKind.DESC) {
//            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);
//            boolean isAsc = operator.getKind() == SqlKind.ASC;
//            return new OrderByElement(expr, isAsc);
//        }

        // 处理算术运算 (+, -, *, /, %)
        if (operator instanceof SqlBinaryOperator && operator.getKind().belongsTo(SqlKind.BINARY_ARITHMETIC)) {
            if (sqlBasicCall.operandCount() == 2) {
                Expr left = (Expr) sqlBasicCall.operand(0).accept(this);
                Expr right = (Expr) sqlBasicCall.operand(1).accept(this);

                ArithmeticExpr.Operator arithOp;
                switch (operator.getKind()) {
                    case PLUS:
                        arithOp = ArithmeticExpr.Operator.ADD;
                        break;
                    case MINUS:
                        arithOp = ArithmeticExpr.Operator.SUBTRACT;
                        break;
                    case TIMES:
                        arithOp = ArithmeticExpr.Operator.MULTIPLY;
                        break;
                    case DIVIDE:
                        arithOp = ArithmeticExpr.Operator.DIVIDE;
                        break;
                    case MOD:
                        arithOp = ArithmeticExpr.Operator.MOD;
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported arithmetic operator: " + operator.getKind());
                }

                return new ArithmeticExpr(arithOp, left, right);
            }
        }

        // 处理一元运算 (+, -)
        if (operator instanceof SqlPrefixOperator && sqlBasicCall.operandCount() == 1) {
            Expr operand = (Expr) sqlBasicCall.operand(0).accept(this);

            if (operator.getKind() == SqlKind.MINUS_PREFIX) {
                // 处理负号
                if (operand instanceof IntLiteral) {
                    return new IntLiteral(-((IntLiteral) operand).getValue());
                } else if (operand instanceof FloatLiteral) {
                    return new FloatLiteral(-((FloatLiteral) operand).getValue());
                } else {
                    return new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY,
                            operand, new IntLiteral(-1));
                }
            } else if (operator.getKind() == SqlKind.PLUS_PREFIX) {
                // 处理正号 (通常可以忽略)
                return operand;
            }
        }

        // 处理逻辑运算 (AND, OR, NOT)
        if ((operator instanceof SqlBinaryOperator &&
                (operator.getKind() == SqlKind.AND || operator.getKind() == SqlKind.OR)) ||
                (operator instanceof SqlPrefixOperator && operator.getKind() == SqlKind.NOT)) {

            if (operator.getKind() == SqlKind.AND || operator.getKind() == SqlKind.OR) {
                Expr left = (Expr) sqlBasicCall.operand(0).accept(this);
                Expr right = (Expr) sqlBasicCall.operand(1).accept(this);

                CompoundPredicate.Operator logicOp = operator.getKind() == SqlKind.AND ?
                        CompoundPredicate.Operator.AND : CompoundPredicate.Operator.OR;

                return new CompoundPredicate(logicOp, left, right);
            } else { // NOT
                Expr operand = (Expr) sqlBasicCall.operand(0).accept(this);
                return new CompoundPredicate(CompoundPredicate.Operator.NOT, operand, null);
            }
        }

        // 处理比较运算 (=, !=, >, <, >=, <=)
        if (operator instanceof SqlBinaryOperator && operator.getKind().belongsTo(SqlKind.COMPARISON)) {
            Expr left = (Expr) sqlBasicCall.operand(0).accept(this);
            Expr right = (Expr) sqlBasicCall.operand(1).accept(this);

            BinaryType comparisonType;
            switch (operator.getKind()) {
                case EQUALS:
                    comparisonType = BinaryType.EQ;
                    break;
                case NOT_EQUALS:
                    comparisonType = BinaryType.NE;
                    break;
                case GREATER_THAN:
                    comparisonType = BinaryType.GT;
                    break;
                case LESS_THAN:
                    comparisonType = BinaryType.LT;
                    break;
                case GREATER_THAN_OR_EQUAL:
                    comparisonType = BinaryType.GE;
                    break;
                case LESS_THAN_OR_EQUAL:
                    comparisonType = BinaryType.LE;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported comparison operator: " + operator.getKind());
            }

            return new BinaryPredicate(comparisonType, left, right);
        }

        // 处理 IN 操作符
        if (operator.getKind() == SqlKind.IN) {
            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);

            if (sqlBasicCall.operand(1) instanceof SqlSelect) {
                // IN 子查询
                Expr subquery = (Expr) sqlBasicCall.operand(1).accept(this);
                return new InPredicate(expr, subquery, false);
            } else {
                // IN 值列表
                List<Expr> inList = new ArrayList<>();
                for (int i = 1; i < sqlBasicCall.operandCount(); i++) {
                    inList.add((Expr) sqlBasicCall.operand(i).accept(this));
                }
                return new InPredicate(expr, inList, false);
            }
        }

        // 处理 NOT IN 操作符
        if (operator.getKind() == SqlKind.NOT_IN) {
            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);

            if (sqlBasicCall.operand(1) instanceof SqlSelect) {
                // NOT IN 子查询
                Expr subquery = (Expr) sqlBasicCall.operand(1).accept(this);
                return new InPredicate(expr, subquery, true);
            } else {
                // NOT IN 值列表
                List<Expr> inList = new ArrayList<>();
                for (int i = 1; i < sqlBasicCall.operandCount(); i++) {
                    inList.add((Expr) sqlBasicCall.operand(i).accept(this));
                }
                return new InPredicate(expr, inList, true);
            }
        }

        // 处理 LIKE 操作符
        if (operator.getKind() == SqlKind.LIKE) {
            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);
            Expr pattern = (Expr) sqlBasicCall.operand(1).accept(this);
            return new LikePredicate(com.starrocks.analysis.LikePredicate.Operator.LIKE, expr, pattern);
        }

//        // 处理 NOT LIKE 操作符
//        if (operator.getKind() == SqlKind.NOT_LIKE) {
//            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);
//            Expr pattern = (Expr) sqlBasicCall.operand(1).accept(this);
//            return new LikePredicate(com.starrocks.analysis.LikePredicate.Operator , expr, pattern, true);
//        }

        // 处理 BETWEEN 操作符
        if (operator.getKind() == SqlKind.BETWEEN) {
            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);
            Expr lower = (Expr) sqlBasicCall.operand(1).accept(this);
            Expr upper = (Expr) sqlBasicCall.operand(2).accept(this);
            return new BetweenPredicate(expr, lower, upper, false);
        }

//        // 处理 NOT BETWEEN 操作符
//        if (operator.getKind() == SqlKind.NOT_BETWEEN) {
//            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);
//            Expr lower = (Expr) sqlBasicCall.operand(1).accept(this);
//            Expr upper = (Expr) sqlBasicCall.operand(2).accept(this);
//            return new BetweenPredicate(expr, lower, upper, true);
//        }

        // 处理 IS NULL 操作符
        if (operator.getKind() == SqlKind.IS_NULL) {
            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);
            return new IsNullPredicate(expr, false);
        }

        // 处理 IS NOT NULL 操作符
        if (operator.getKind() == SqlKind.IS_NOT_NULL) {
            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);
            return new IsNullPredicate(expr, true);
        }

        // 处理 CAST 操作符
        if (operator.getKind() == SqlKind.CAST) {
            Expr expr = (Expr) sqlBasicCall.operand(0).accept(this);
            TypeDef typeDef = (TypeDef) sqlBasicCall.operand(1).accept(this);
            return new CastExpr(typeDef, expr);
        }

        throw new UnsupportedOperationException("Unsupported SqlBasicCall operator: " + sqlBasicCall.getOperator());
    }

    private ParseNode visitFunction(SqlBasicCall sqlBasicCall) {
        List<Expr> arguments = new ArrayList<>();
        for (SqlNode operand : sqlBasicCall.getOperandList()) {
            arguments.add((Expr) operand.accept(this));
        }

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
            // 处理聚合函数
            SqlAggFunction aggFunction = null;
            if (sqlBasicCall.getOperator() instanceof SqlAggFunction) {
                aggFunction = (SqlAggFunction) sqlBasicCall.getOperator();
            }

            if (aggFunction != null) {
                boolean isDistinct = false;

                // 检查是否有DISTINCT关键字
                if (sqlBasicCall.getFunctionQuantifier() != null &&
                        sqlBasicCall.getFunctionQuantifier().toString().equalsIgnoreCase("DISTINCT")) {
                    isDistinct = true;
                }

                FunctionCallExpr functionCallExpr = new FunctionCallExpr(
                        functionName,
                        new FunctionParams(isDistinct, arguments));

                return SyntaxSugars.parse(functionCallExpr);
            } else {
                FunctionCallExpr functionCallExpr = new FunctionCallExpr(
                        functionName,
                        new FunctionParams(false, arguments));

                return SyntaxSugars.parse(functionCallExpr);
            }
        }
    }
}