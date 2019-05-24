package org.gradoop.flink.algorithms.gelly.partitioning;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.BaseGellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.partitioning.functions.ARPMessage;
import org.gradoop.flink.algorithms.gelly.partitioning.functions.ARPUpdate;
import org.gradoop.flink.algorithms.gelly.partitioning.functions.AddPartitionProperty;
import org.gradoop.flink.algorithms.gelly.partitioning.functions.InitializeARPVertex;
import org.gradoop.flink.algorithms.gelly.partitioning.functions.LongIdTupleToGellyEdgeWithNullValueJoin;
import org.gradoop.flink.algorithms.gelly.partitioning.functions.PrepareResultTuple;
import org.gradoop.flink.algorithms.gelly.partitioning.tuples.ARPVertexValue;
import org.gradoop.flink.algorithms.gelly.randomjump.KRandomJumpGellyVCI;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.LongIdWithEdgeToTupleJoin;
import org.gradoop.flink.algorithms.gelly.randomjump.functions.ReplaceTargetWithLongIdJoin;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;

import java.util.Map;

public class GradoopARPartitioning extends BaseGellyAlgorithm<Long, ARPVertexValue, NullValue, LogicalGraph> {

  /**
   * The graph used in {@link KRandomJumpGellyVCI#execute(LogicalGraph)}.
   */
  protected LogicalGraph currentGraph;

  public static final String CAPACITY_AGGREGATOR_PREFIX = "load.";

  public static final String DEMAND_AGGREGATOR_PREFIX = "demand.";

  private final String PROPERTY_KEY;

  private final int maxIteration;

  private final int numPartitions;

  DataSet<Tuple2<Long, GradoopId>> vertexIdsMap;

  DataSet<Tuple2<Long, GradoopId>> edgeIdsMap;


  public GradoopARPartitioning(int numPartitions, String propertyKey) {
    this.numPartitions = numPartitions;
    this.maxIteration = 100;
    this.PROPERTY_KEY = propertyKey;
  }

  public GradoopARPartitioning(int numPartitions, int maxIteration, String propertyKey){
    this.numPartitions = numPartitions;
    this.maxIteration = maxIteration;
    this.PROPERTY_KEY = propertyKey;
  }


  @Override
  public Graph<Long, ARPVertexValue, NullValue> transformToGelly(LogicalGraph graph) {

    this.currentGraph = graph;

    vertexIdsMap = DataSetUtils.zipWithUniqueId(graph.getVertices().map(new Id<>()));

    edgeIdsMap = DataSetUtils.zipWithUniqueId(graph.getEdges().map(new Id<>()));

    DataSet<Vertex<Long, ARPVertexValue>> gellyVertices =
      vertexIdsMap.map(new InitializeARPVertex(numPartitions));

    DataSet<Edge<Long, NullValue>> gellyEdges = graph.getEdges()
      .join(vertexIdsMap)
      .where(new SourceId<>()).equalTo(1)
      .with(new LongIdWithEdgeToTupleJoin())
      .join(vertexIdsMap)
      .where(1).equalTo(1)
      .with(new ReplaceTargetWithLongIdJoin())
      .join(edgeIdsMap)
      .where(2).equalTo(1)
      .with(new LongIdTupleToGellyEdgeWithNullValueJoin());

    return Graph.fromDataSet(gellyVertices, gellyEdges, graph.getConfig().getExecutionEnvironment());
  }

  @Override
  public LogicalGraph executeInGelly(Graph<Long, ARPVertexValue, NullValue> graph) {

    ScatterGatherConfiguration configuration = createVCIParams();

    Graph<Long, ARPVertexValue, NullValue> resultGraph = graph.runScatterGatherIteration(
      new ARPMessage(), new ARPUpdate(numPartitions), maxIteration, configuration);

    DataSet<org.gradoop.common.model.impl.pojo.Vertex> updatedVertices = resultGraph.getVertices()
      .join(vertexIdsMap)
      .where(0).equalTo(0)
      .with(new PrepareResultTuple())
      .join(currentGraph.getVertices())
      .where(1).equalTo(new Id<>())
      .with(new AddPartitionProperty(PROPERTY_KEY));

    return currentGraph.getFactory()
      .fromDataSets(currentGraph.getGraphHead(), updatedVertices, currentGraph.getEdges());
  }



  private ScatterGatherConfiguration createVCIParams() {

    ScatterGatherConfiguration configuration = new ScatterGatherConfiguration();

    for (int i = 0; i < numPartitions; i++) {
      configuration.registerAggregator(CAPACITY_AGGREGATOR_PREFIX + i, new LongSumAggregator());
      configuration.registerAggregator(DEMAND_AGGREGATOR_PREFIX + i, new LongSumAggregator());
    }

    for (Map.Entry<String, Aggregator<?>> value :configuration.getAggregators().entrySet()) {
      System.out.println(value.getKey());
      System.out.println(value.getValue());
    }

    return configuration;
  }
}
