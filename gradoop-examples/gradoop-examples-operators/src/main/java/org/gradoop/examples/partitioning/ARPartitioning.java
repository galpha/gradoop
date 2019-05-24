package org.gradoop.examples.partitioning;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.algorithms.gelly.partitioning.GradoopARPartitioning;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.deprecated.logicalgraphcsv.LogicalGraphCSVDataSource;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class ARPartitioning {

  public static void main(String[] args) throws Exception {

    String input = args[0];

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    DataSource source = new LogicalGraphCSVDataSource(input,config);

    LogicalGraph testGraph = source.getLogicalGraph();

    LogicalGraph resultGraph = new GradoopARPartitioning(2, 50, "partition").execute(testGraph);

    String output = args[1];

    DataSink sink = new DOTDataSink(output, false, DOTDataSink.DotFormat.SIMPLE);
    sink.write(resultGraph);

    env.execute();

  }

}
