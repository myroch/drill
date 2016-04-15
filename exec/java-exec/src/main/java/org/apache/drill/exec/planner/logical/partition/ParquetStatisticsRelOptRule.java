package org.apache.drill.exec.planner.logical.partition;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.ParquetPartitionDescriptor;
import org.apache.drill.exec.planner.PartitionDescriptor;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

public class ParquetStatisticsRelOptRule extends PruneScanRule {

  public ParquetStatisticsRelOptRule(OptimizerRulesContext optimizerRulesContext) {
    super(RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
        "PruneScanRule:Filter_On_Parquet_Statistics", optimizerRulesContext);
  }

  @Override
  public PartitionDescriptor getPartitionDescriptor(PlannerSettings settings, TableScan scanRel) {
    return new ParquetStatisticsPartitionDescriptor(settings, (ParquetStatisticsDrillScanRel) scanRel);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final DrillScanRel scan = call.rel(1);
    GroupScan groupScan = scan.getGroupScan();
    return groupScan instanceof ParquetGroupScan && !(scan instanceof ParquetStatisticsDrillScanRel);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillFilterRel filterRel = call.rel(0);
    final DrillScanRel scanRel = call.rel(1);

    RexNode condition = filterRel.getCondition();
    RewriteConditionsToStatistics visitor = new RewriteConditionsToStatistics(true, filterRel.getCluster().getRexBuilder(),
        scanRel.getRowType().getFieldList(), (ParquetGroupScan) scanRel.getGroupScan());
    RexNode newCondition = condition.accept(visitor);
    if (condition.toString().equals(newCondition.toString())) {
      return; // no need to go further, as we don't have query on statistics
    }
    RelDataType newRowType = new RelRecordType(visitor.getFieldList());

    final RelOptRuleCall newCall = new ParquetStatisticsHepRuleCall(call);
    final Filter newFilterRel = new DrillFilterRel(filterRel.getCluster(), filterRel.getTraitSet(), filterRel.getInput(), newCondition);
    final TableScan newScanRel = new ParquetStatisticsDrillScanRel(scanRel, newRowType);

    doOnMatch(newCall, newFilterRel, null, newScanRel);
  }

  public static ParquetStatisticsRelOptRule getInstance(OptimizerRulesContext optimizerRulesContext) {
    return new ParquetStatisticsRelOptRule(optimizerRulesContext);
  }

  private static class ParquetStatisticsHepRuleCall extends RelOptRuleCall {
    private final RelOptRuleCall call;
    private final DrillFilterRel filterRel;

    protected ParquetStatisticsHepRuleCall(RelOptRuleCall call) {
      super(call.getPlanner(), call.getOperand0(), call.getRelList().toArray(new RelNode[0]), new HashMap<RelNode, List<RelNode>>(), null);
      this.call = call;
      filterRel = call.rel(0);
    }

    @Override
    public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
      if (rel instanceof DrillFilterRel) {
        DrillFilterRel filter = (DrillFilterRel) rel;
        // using original filterRel condition, so newly added min and max columns are removed
        final RelNode newFilter = new DrillFilterRel(filter.getCluster(), filter.getTraitSet(), filter.getInput(), this.filterRel.getCondition());
        this.call.transformTo(newFilter, equiv);
      } else {
        // filter was dropped as statistics are more generic than actual values, bring it back
        RelNode newFilter = this.filterRel.copy(filterRel.getTraitSet(), Collections.singletonList(rel));
        this.call.transformTo(newFilter, equiv);
      }
    }
  }

  private static class ParquetStatisticsDrillScanRel extends DrillScanRel {
    private final DrillScanRel scanRel;

    private ParquetStatisticsDrillScanRel(DrillScanRel scanRel, RelDataType rowType) {
      super(scanRel.getCluster(), scanRel.getTraitSet(), scanRel.getTable(), scanRel.getGroupScan(), rowType,
          scanRel.getColumns(), scanRel.partitionFilterPushdown());
      this.scanRel = scanRel;
    }

    public DrillScanRel getFormerScanRel() {
      return scanRel;
    }
  }

  private static class ParquetStatisticsPartitionDescriptor extends ParquetPartitionDescriptor {
    private final RelDataType rowType;

    private ParquetStatisticsPartitionDescriptor(PlannerSettings settings, ParquetStatisticsDrillScanRel scanRel) {
      super(settings, scanRel);
      this.rowType = scanRel.getFormerScanRel().getRowType();
    }

    @Override
    public TableScan createTableScan(List<String> newFiles) throws Exception {
      DrillScanRel newScanRel = (DrillScanRel) super.createTableScan(newFiles);
      // using original row type, so newly added min and max columns are removed
      return new ParquetStatisticsDrillScanRel(newScanRel, this.rowType);
    }
  }
}
