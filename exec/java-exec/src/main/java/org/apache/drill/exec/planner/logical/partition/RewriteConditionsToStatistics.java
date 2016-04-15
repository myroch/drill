package org.apache.drill.exec.planner.logical.partition;

import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class RewriteConditionsToStatistics extends RexVisitorImpl<RexNode> {

  private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
  private static final RelDataType BOOLEAN_TYPE = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);

  private final RexBuilder builder;
  private final List<RelDataTypeField> fieldList;
  private final ParquetGroupScan parquetGroupScan;

  public RewriteConditionsToStatistics(boolean deep, RexBuilder builder, List<RelDataTypeField> fieldList,
      ParquetGroupScan parquetGroupScan) {
    super(deep);
    this.builder = builder;
    this.fieldList = new ArrayList<>(fieldList);
    this.parquetGroupScan = parquetGroupScan;
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    return inputRef;
  }

  @Override
  public RexNode visitLiteral(RexLiteral literal) {
    return literal;
  }

  @Override
  public RexNode visitOver(RexOver over) {
    return over;
  }

  @Override
  public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
    return correlVariable;
  }

  @Override
  public RexNode visitCall(RexCall call) {
    SqlOperator op = call.getOperator();
    SqlKind kind = op.getKind();
    RelDataType type = call.getType();
    if (kind == EQUALS || kind == GREATER_THAN || kind == GREATER_THAN_OR_EQUAL || kind == LESS_THAN
        || kind == LESS_THAN_OR_EQUAL) {
      if (call.getOperands().size() == 2) {
        RexNode op1 = call.getOperands().get(0);
        RexNode op2 = call.getOperands().get(1);
        RexInputRef ref;
        RexLiteral literal;
        if (op1 instanceof RexInputRef && op2 instanceof RexLiteral) {
          ref = (RexInputRef) op1;
          literal = (RexLiteral) op2;
        } else if (op2 instanceof RexInputRef && op1 instanceof RexLiteral) {
          ref = (RexInputRef) op2;
          literal = (RexLiteral) op1;
        } else {
          return builder.makeCall(type, op, visitChildren(call));
        }
        if (this.fieldList.size() > ref.getIndex()) { // should be always true
          RelDataTypeField field = this.fieldList.get(ref.getIndex());
          String fieldName = field.getName();
          SchemaPath schemaPath = SchemaPath.getSimplePath(fieldName + ParquetGroupScan.SUFFIX_MIN);
          if (parquetGroupScan.getTypeForColumn(schemaPath) != null) {
            // rewrite condition, we have statistics contained
            if (kind == EQUALS) {
              // A = C becomes A_min <= C AND A_max >= C
              RexNode left = builder.makeCall(BOOLEAN_TYPE, SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                  getDeductedFieldArgs(field, ParquetGroupScan.SUFFIX_MIN, ref, literal));
              RexNode right = builder.makeCall(BOOLEAN_TYPE, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                  getDeductedFieldArgs(field, ParquetGroupScan.SUFFIX_MAX, ref, literal));
              return builder.makeCall(BOOLEAN_TYPE, SqlStdOperatorTable.AND, ImmutableList.of(left, right));
            } else if (kind == GREATER_THAN || kind == GREATER_THAN_OR_EQUAL) {
              // A > C becomes A_max > C
              return builder.makeCall(BOOLEAN_TYPE, op, getDeductedFieldArgs(field, ParquetGroupScan.SUFFIX_MAX, ref, literal));
            } else if (kind == LESS_THAN || kind == LESS_THAN_OR_EQUAL) {
              // A < C becomes A_min < C
              return builder.makeCall(BOOLEAN_TYPE, op, getDeductedFieldArgs(field, ParquetGroupScan.SUFFIX_MIN, ref, literal));
            }
          }
        }
      }
    }
    return builder.makeCall(type, op, visitChildren(call));
  }

  private List<RexNode> visitChildren(RexCall call) {
    List<RexNode> children = Lists.newArrayList();
    for (RexNode child : call.getOperands()) {
      children.add(child.accept(this));
    }
    return ImmutableList.copyOf(children);
  }

  private List<RexNode> getDeductedFieldArgs(RelDataTypeField field, String postFix, RexInputRef ref,
      RexLiteral literal) {
    String fieldName = field.getName();
    String newFieldName = fieldName + postFix;
    int foundIdx = -1, fieldsLen = this.fieldList.size();
    for (int i = 0; i < fieldsLen; i++) {
      if (newFieldName.equals(this.fieldList.get(i).getName())) {
        foundIdx = i;
        break;
      }
    }
    if (foundIdx < 0) {
      foundIdx = fieldsLen;
      this.fieldList.add(new RelDataTypeFieldImpl(newFieldName, foundIdx, field.getType()));
    }
    RexInputRef newRef = new RexInputRef(foundIdx, ref.getType());
    return ImmutableList.of(newRef, literal);
  }

  @Override
  public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
    return dynamicParam;
  }

  @Override
  public RexNode visitRangeRef(RexRangeRef rangeRef) {
    return rangeRef;
  }

  @Override
  public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    return fieldAccess;
  }

  @Override
  public RexNode visitLocalRef(RexLocalRef localRef) {
    return localRef;
  }

  public List<RelDataTypeField> getFieldList() {
    return fieldList;
  }
}
