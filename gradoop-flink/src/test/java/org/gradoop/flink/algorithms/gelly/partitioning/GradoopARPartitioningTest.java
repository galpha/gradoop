package org.gradoop.flink.algorithms.gelly.partitioning;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class GradoopARPartitioningTest extends GradoopFlinkTestBase {

  private String getGDLString() {
    return
        "(a:A)-->(b:B)-->(c:C)-->(d:D)-->(a)" +
        "(d)-->(b)" +
        "(a)-->(e:E)-->(f:F)-->(a)";
  }

  @Test
  public void testGraph() throws Exception {

    String input = "/home/galpha/datasets/gradoop/facebook_gradoop_csv/";

    DataSource source = new LogicalGraphCSVDataSource(input, getConfig());

//    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());
//    loader.initDatabaseFromString(getGDLString());

    LogicalGraph testGraph = source.getLogicalGraph();

    LogicalGraph resultGraph = new GradoopARPartitioning(2, 1, "partition").execute(testGraph);

    String output = "/home/galpha/datasets/gradoop/facebook_partitioned.dot";

    DataSink sink = new DOTDataSink(output, false, DOTDataSink.DotFormat.SIMPLE);
    sink.write(resultGraph);

    getExecutionEnvironment().execute();
  }

}
