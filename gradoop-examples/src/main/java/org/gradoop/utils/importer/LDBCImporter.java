package org.gradoop.utils.importer;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.utils.importer.functions.LDBCEdgeToEPGMEdge;
import org.gradoop.utils.importer.functions.LDBCVertexToEPGMVertex;
import org.s1ck.ldbc.LDBCToFlink;

public class LDBCImporter extends AbstractRunner implements ProgramDescription {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    LDBCToFlink ldbcToFlink = new LDBCToFlink(args[0], env);
    DataSet<ImportVertex<Long>> vertices = ldbcToFlink.getVertices()
      .map(new LDBCVertexToEPGMVertex());

    DataSet<ImportEdge<Long>> edges = ldbcToFlink.getEdges()
      .map(new LDBCEdgeToEPGMEdge());

    GraphDataSource<Long> source = new GraphDataSource<>(vertices, edges, config);

    DataSink sink = new CSVDataSink(args[1], config);

    sink.write(source.getLogicalGraph());

    env.execute("LDBC-Import");
  }

  @Override
  public String getDescription() {
    return LDBCImporter.class.getName();
  }
}
