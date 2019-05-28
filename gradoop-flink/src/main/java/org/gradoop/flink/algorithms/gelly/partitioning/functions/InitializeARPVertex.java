package org.gradoop.flink.algorithms.gelly.partitioning.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.partitioning.tuples.ARPVertexValue;

public class InitializeARPVertex implements MapFunction<Tuple3<Long, GradoopId, Long>,
  Vertex<Long,
  ARPVertexValue>> {

  private Vertex<Long, ARPVertexValue> reuse;
  private int numPartitions;

  public InitializeARPVertex(int numPartitions) {
    this.reuse = new Vertex<>();
    this.numPartitions = numPartitions;
  }

  @Override
  public Vertex<Long, ARPVertexValue> map(Tuple3<Long, GradoopId, Long> tuple) throws Exception {

    ARPVertexValue value = new ARPVertexValue(tuple.f0 % numPartitions, Long.MAX_VALUE, tuple.f2);

    reuse.setId(tuple.f0);
    reuse.setValue(value);

    return reuse;
  }
}
