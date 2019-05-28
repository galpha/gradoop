package org.gradoop.flink.algorithms.gelly.partitioning.functions;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.types.LongValue;
import org.gradoop.flink.algorithms.gelly.partitioning.GradoopARPartitioning;
import org.gradoop.flink.algorithms.gelly.partitioning.tuples.ARPVertexValue;

import java.util.Random;

public class ARPUpdate extends GatherFunction<Long, ARPVertexValue, Long> {

  /**
   * final long value +1
   */
  private final long POSITIVE_ONE = 1;
  /**
   * final long value -1
   */
  private final long NEGATIVE_ONE = -1;

  private final int k;

  private double capacityThreshold;

  private Random random;

  private String CAPACITY_AGGREGATOR_PREFIX = GradoopARPartitioning.CAPACITY_AGGREGATOR_PREFIX;
  private String DEMAND_AGGREGATOR_PREFIX = GradoopARPartitioning.DEMAND_AGGREGATOR_PREFIX;

  public ARPUpdate(int numPartitions) {
    this.k = numPartitions;
    this.capacityThreshold = 0.1;
    this.random = new Random();
  }

  public ARPUpdate(int numPartitions, double capacityThreshold) {
    this.k = numPartitions;
    this.capacityThreshold = capacityThreshold;
    this.random = new Random();
  }


//  @Override
//  public void updateVertex(Vertex<Long, ARPVertexValue> vertex,
//    MessageIterator<Long> msg) throws Exception {
//    System.out.println(getSuperstepNumber());
//    if (getSuperstepNumber() == 1) {
//      long newValue = vertex.getId() % k;
//      notifyAggregator(getCapacityAggregatorString(newValue), POSITIVE_ONE);
//      setNewVertexValue(new ARPVertexValue(newValue, Long.MAX_VALUE));
//    } else {
//      //odd numbered superstep 3 (migrate)
//      if ((getSuperstepNumber() % 2) == 1) {
//        long desiredPartition = vertex.getValue().getDesiredPartition();
//        long currentPartition = vertex.getValue().getCurrentPartition();
//        if (desiredPartition != currentPartition) {
//          boolean migrate = doMigrate(desiredPartition);
//          if (migrate) {
//            migrateVertex(vertex, desiredPartition);
//          } else {
//            setNewVertexValue(vertex.getValue());
//          }
//        }
//        notifyAggregator(getCapacityAggregatorString(currentPartition), POSITIVE_ONE);
//        //even numbered superstep 2 (demand)
//      } else if ((getSuperstepNumber() % 2) == 0) {
//        long desiredPartition = getDesiredPartition(vertex, msg);
//        long currentPartition = vertex.getValue().getCurrentPartition();
//        boolean changed = currentPartition != desiredPartition;
//        if (changed) {
//          notifyAggregator(getDemandAggregatorString(desiredPartition), POSITIVE_ONE);
//          setNewVertexValue(new ARPVertexValue(currentPartition, desiredPartition));
//        }
//        notifyAggregator(getCapacityAggregatorString(currentPartition), POSITIVE_ONE);
//      }
//    }
//  }

  @Override
  public void updateVertex(Vertex<Long, ARPVertexValue> vertex,
    MessageIterator<Long> msg) throws Exception {
    System.out.println(getSuperstepNumber());
    // demand
    if ((getSuperstepNumber() % 2) == 1) {
      long currentPartition = vertex.getValue().getCurrentPartition();
      long desiredPartition = getDesiredPartition(vertex, msg);
//      boolean changed = currentPartition != desiredPartition;
//      if (changed) {
        notifyAggregator(getDemandAggregatorString(desiredPartition), POSITIVE_ONE);
        setNewVertexValue(new ARPVertexValue(currentPartition, desiredPartition));
//      }

    // migrate
    } else if ((getSuperstepNumber() % 2) == 0) {

      long desiredPartition = vertex.getValue().getDesiredPartition();
      long currentPartition = vertex.getValue().getCurrentPartition();

      System.out.println("knoten:" + vertex.getId() + " currentPart:" + currentPartition + " " +
        "desiredPart:" + desiredPartition);

      if (desiredPartition != currentPartition) {
        boolean migrate = doMigrate(desiredPartition);
        if (migrate) {
          migrateVertex(vertex, desiredPartition);
        }
      }
    }
  }

  /**
   * Notify Aggregator
   *
   * @param aggregatorString Aggregator name
   * @param v                value
   */
  private void notifyAggregator(final String aggregatorString, final long v) {
    LongSumAggregator aggregator = getIterationAggregator(aggregatorString);
    aggregator.aggregate(v);
  }

  /**
   * Calculates the partition the vertex wants to migrate to
   *
   * @param vertex   actual vertex
   * @param messages all messages
   * @return desired partition id the vertex wants to migrate to
   */
  private long getDesiredPartition(final Vertex<Long, ARPVertexValue> vertex,
    final Iterable<Long> messages) {
    long currentPartition = vertex.getValue().getCurrentPartition();
    long desiredPartition = vertex.getValue().getDesiredPartition();
    // got messages?
    if (messages.iterator().hasNext()) {
      // partition -> neighbours in partition i
      long[] countNeighbours = getPartitionFrequencies(messages);
      // partition -> desire to migrate

      long outDegree = vertex.getValue().getDegree();
      double[] partitionWeights = getPartitionWeights(countNeighbours, outDegree);
      double firstMax = Integer.MIN_VALUE;
      double secondMax = Integer.MIN_VALUE;
      int firstK = -1;
      int secondK = -1;
      for (int i = 0; i < k; i++) {
        if (partitionWeights[i] > firstMax) {
          secondMax = firstMax;
          firstMax = partitionWeights[i];
          secondK = firstK;
          firstK = i;
        } else if (partitionWeights[i] > secondMax) {
          secondMax = partitionWeights[i];
          secondK = i;
        }
      }
      if (firstMax == secondMax) {
        if (currentPartition != firstK && currentPartition != secondK) {
          return firstK;
        } else {
          return currentPartition;
        }
      } else {
        return firstK;
      }
    }
    // no massages (degree = 0) -> return current partition
    return currentPartition;
  }

  /**
   * Counts the partitions in the neighborhood of the vertex
   *
   * @param messages all recieved messages
   * @return array with all partitions in the neighborhood and counted how
   * often they are there
   */
  private long[] getPartitionFrequencies(final Iterable<Long> messages) {
    long[] result = new long[k];
    for (Long message : messages) {
      result[message.intValue()]++;
    }
    return result;
  }

  /**
   * calculates the partition weights. The node will try to migrate into
   * the partition with the highest weight.
   *
   * @param partitionFrequencies array with all neighbor partitions
   * @param numEdges             total number of edges of this vertex
   * @return calculated partition weights
   */
  private double[] getPartitionWeights(long[] partitionFrequencies,
    long numEdges) {
    double[] partitionWeights = new double[k];
    for (int i = 0; i < k; i++) {
      long load = getAggregatedValue(getCapacityAggregatorString(i));
      long freq = partitionFrequencies[i];
      double weight = (double) freq / (load * numEdges);
      partitionWeights[i] = weight;
    }
    return partitionWeights;
  }

  /**
   * Moves a vertex from its old to its new partition.
   *
   * @param vertex           vertex
   * @param desiredPartition partition to move vertex to
   */
  private void migrateVertex(final Vertex<Long, ARPVertexValue> vertex, long desiredPartition) {
    // decrease capacity in old partition
    notifyAggregator(getCapacityAggregatorString(vertex.getValue().getCurrentPartition()), NEGATIVE_ONE);
    // increase capacity in new partition
    notifyAggregator(getCapacityAggregatorString(desiredPartition), POSITIVE_ONE);

    setNewVertexValue(new ARPVertexValue(desiredPartition, vertex.getValue().getDesiredPartition()));
  }

  /**
   * Decides of a vertex is allowed to migrate to a given desired partition.
   * This is based on the free space in the partition and the demand for that
   * partition.
   *
   * @param desiredPartition desired partition
   * @return true if the vertex is allowed to migrate, false otherwise
   */
  private boolean doMigrate(long desiredPartition) {
    long totalCapacity = getTotalCapacity();
    long load = getAggregatedValue(getCapacityAggregatorString(desiredPartition));
    long availability = totalCapacity - load;
    long demand = getAggregatedValue(getDemandAggregatorString(desiredPartition));
    double threshold = (double) availability / demand;
    double randomRange = random.nextDouble();
    return Double.compare(randomRange, threshold) < 0;
  }

  /**
   * Returns the total number of vertices a partition can store. This depends
   * on the strict capacity and the capacity threshold.
   *
   * @return total capacity of a partition
   */
  private int getTotalCapacity() {
    double strictCapacity = getNumberOfVertices() / (double) k;
    double buffer = strictCapacity * capacityThreshold;
    return (int) Math.ceil(strictCapacity + buffer);
  }

  /**
   * Return the aggregated value of the previous super-step
   *
   * @param agg aggregator name
   * @return aggregated value
   */
  private long getAggregatedValue(String agg) {
    LongValue aggregatedValue = getPreviousIterationAggregate(agg);

    System.out.println(agg);

    if (aggregatedValue == null) {
      LongSumAggregator aggregator = getIterationAggregator(agg);
      return aggregator.getAggregate().getValue();
    }

    return aggregatedValue.getValue();
  }


  private String getDemandAggregatorString(long desiredPartition) {
    return DEMAND_AGGREGATOR_PREFIX + desiredPartition;
  }

  private String getCapacityAggregatorString(long desiredPartition) {
    return CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
  }
}
