/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.storage.hbase.impl.io;

import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.impl.io.functions.BuildEdgeMutation;
import org.gradoop.storage.hbase.impl.io.functions.BuildGraphHeadMutation;
import org.gradoop.storage.hbase.impl.io.functions.BuildVertexMutation;
import org.gradoop.storage.hbase.impl.HBaseEPGMStore;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Converts runtime representation of EPGM elements into persistent
 * representations and writes them to HBase.
 */
public class HBaseDataSink extends HBaseBase implements DataSink {

  /**
   * Creates a new HBase data sink.
   *
   * @param epgmStore store implementation
   * @param flinkConfig gradoop flink execute config
   */
  public HBaseDataSink(
    @Nonnull HBaseEPGMStore epgmStore,
    @Nonnull GradoopFlinkConfig flinkConfig
  ) {
    super(epgmStore, flinkConfig);
  }

  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph, false);
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(graphCollection, false);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {
    write(logicalGraph.getCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {
    if (overWrite) {
      getStore().truncateTables();
    }

    // transform graph data to persistent graph data and write it
    writeGraphHeads(graphCollection);

    // transform vertex data to persistent vertex data and write it
    writeVertices(graphCollection);

    // transform edge data to persistent edge data and write it
    writeEdges(graphCollection);
  }

  /**
   * Converts runtime graph data to persistent graph data (including vertex
   * and edge identifiers) and writes it to HBase.
   *
   * @param collection Graph collection
   * @throws IOException if fetching mapreduce instance failed
   */
  private void writeGraphHeads(final GraphCollection collection)
    throws IOException {

    // write (graph-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getGraphTableName().getNameAsString());

    collection.getGraphHeads()
      .map(new BuildGraphHeadMutation(getHBaseConfig().getGraphHeadHandler()))
      .output(new HadoopOutputFormat<>(new TableOutputFormat<>(), job));
  }

  /**
   * Converts runtime vertex data to persistent vertex data (includes
   * incoming and outgoing edge data) and writes it to HBase.
   *
   * @param collection Graph collection
   * @throws IOException if fetching mapreduce instance failed
   */
  private void writeVertices(final GraphCollection collection) throws IOException {

    // write (vertex-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getVertexTableName().getNameAsString());

    collection.getVertices()
      .map(new BuildVertexMutation(getHBaseConfig().getVertexHandler()))
      .output(new HadoopOutputFormat<>(new TableOutputFormat<>(), job));
  }

  /**
   * Converts runtime edge data to persistent edge data (includes
   * source/target vertex data) and writes it to HBase.
   *
   * @param collection Graph collection
   * @throws IOException if fetching mapreduce instance failed
   */
  private void writeEdges(final GraphCollection collection) throws IOException {

    // write (edge-data) to HBase table
    Job job = Job.getInstance();
    job.getConfiguration()
      .set(TableOutputFormat.OUTPUT_TABLE, getHBaseConfig().getEdgeTableName().getNameAsString());

    collection.getEdges()
      .map(new BuildEdgeMutation(getHBaseConfig().getEdgeHandler()))
      .output(new HadoopOutputFormat<>(new TableOutputFormat<>(), job));
  }
}
