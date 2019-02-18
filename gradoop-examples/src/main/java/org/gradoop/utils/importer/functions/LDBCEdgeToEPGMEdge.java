package org.gradoop.utils.importer.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.s1ck.ldbc.tuples.LDBCEdge;

public class LDBCEdgeToEPGMEdge implements MapFunction<LDBCEdge, ImportEdge<Long>> {

  private final ImportEdge<Long> reuse;

  public LDBCEdgeToEPGMEdge() {
    reuse = new ImportEdge<>();
  }

  @Override
  public ImportEdge<Long> map(LDBCEdge ldbcEdge) throws Exception {

    reuse.setId(ldbcEdge.getEdgeId());
    reuse.setLabel(ldbcEdge.getLabel());
    reuse.setSourceId(ldbcEdge.getSourceVertexId());
    reuse.setTargetId(ldbcEdge.getTargetVertexId());
    reuse.setProperties(Properties.createFromMap(ldbcEdge.getProperties()));

    return reuse;
  }
}
