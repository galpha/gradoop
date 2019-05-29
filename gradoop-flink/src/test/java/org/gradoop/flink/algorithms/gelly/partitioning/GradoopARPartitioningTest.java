package org.gradoop.flink.algorithms.gelly.partitioning;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.RandomEdgeSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexSampling;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class GradoopARPartitioningTest extends GradoopFlinkTestBase {

  @Test
  public void testGraph() throws Exception {

    String input = "/home/galpha/datasets/gradoop/facebook_gradoop_csv/";

    DataSource source = new LogicalGraphCSVDataSource(input, getConfig());

    LogicalGraph testGraph = source.getLogicalGraph();

    LogicalGraph resultGraph =
      new GradoopARPartitioning(2, 50, 0.05, "partition").execute(testGraph);

    String output = "/home/galpha/datasets/gradoop/facebook_partitioned.dot";

    DataSink sink = new DOTDataSink(output, false, DOTDataSink.DotFormat.SIMPLE);
    sink.write(resultGraph);

    getExecutionEnvironment().execute();
  }

  @Test
  public void sampleGraph() throws Exception {

    String input = "/home/galpha/datasets/gradoop/graphalytics_1/";

    DataSource source = new LogicalGraphCSVDataSource(input, getConfig());

    LogicalGraph testGraph = source.getLogicalGraph();

    LogicalGraph sampledGraph = testGraph.sample(new RandomEdgeSampling(0.01f));

    String output = "/home/galpha/datasets/gradoop/graphalytics_1_001_edge_sample/";

    DataSink sink = new CSVDataSink(output, getConfig());
    sink.write(sampledGraph);

    getExecutionEnvironment().execute();
  }

}
