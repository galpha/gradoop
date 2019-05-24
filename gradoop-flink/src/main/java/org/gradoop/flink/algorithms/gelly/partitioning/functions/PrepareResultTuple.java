package org.gradoop.flink.algorithms.gelly.partitioning.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.partitioning.tuples.ARPVertexValue;

public class PrepareResultTuple
  implements JoinFunction<Vertex<Long, ARPVertexValue>, Tuple3<Long, GradoopId, Long>,
  Tuple3<Long, GradoopId, Long>> {

  private Tuple3<Long, GradoopId, Long> reuse;

  public PrepareResultTuple() {
    this.reuse = new Tuple3<>();
  }

  @Override
  public Tuple3<Long, GradoopId, Long> join(Vertex<Long, ARPVertexValue> vertex,
    Tuple3<Long, GradoopId, Long> vertexIdMap) throws Exception {

    reuse.f0 = vertexIdMap.f0;
    reuse.f1 = vertexIdMap.f1;
    reuse.f2 = vertex.getValue().getCurrentPartition();

    return reuse;
  }
}
