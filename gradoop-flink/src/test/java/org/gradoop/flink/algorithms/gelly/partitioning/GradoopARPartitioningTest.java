package org.gradoop.flink.algorithms.gelly.partitioning;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.junit.Test;

public class GradoopARPartitioningTest extends GradoopFlinkTestBase {

  @Test
  public void testGraph() throws Exception {

    LogicalGraph testGraph = getSocialNetworkLoader().getLogicalGraph();

    LogicalGraph resultGraph = new GradoopARPartitioning(2, "partition").execute(testGraph);

    resultGraph.print();

  }

}
