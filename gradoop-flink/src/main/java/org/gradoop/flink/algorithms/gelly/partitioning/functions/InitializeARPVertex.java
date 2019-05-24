package org.gradoop.flink.algorithms.gelly.partitioning.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.partitioning.tuples.ARPVertexValue;

public class InitializeARPVertex implements MapFunction<Tuple2<Long, GradoopId>, Vertex<Long, ARPVertexValue>> {

  private Vertex<Long, ARPVertexValue> reuse;
  private int numPartitions;

  public InitializeARPVertex(int numPartitions) {
    this.numPartitions = numPartitions;
    this.reuse = new Vertex<>();
  }

  @Override
  public Vertex<Long, ARPVertexValue> map(Tuple2<Long, GradoopId> tuple) throws Exception {

    long initValue = tuple.f0 % numPartitions;

    ARPVertexValue value = new ARPVertexValue(Long.MAX_VALUE, Long.MAX_VALUE);

    reuse.setId(tuple.f0);
    reuse.setValue(value);

    return reuse;
  }
}
