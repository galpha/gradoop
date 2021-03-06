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
package org.gradoop.storage.impl.hbase.predicate.filter.impl;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Type;
import org.gradoop.storage.hbase.impl.predicate.filter.impl.HBasePropReg;
import org.testng.annotations.Test;

import java.util.regex.Pattern;

import static org.gradoop.storage.hbase.impl.constants.HBaseConstants.CF_PROPERTY_TYPE;
import static org.gradoop.storage.hbase.impl.constants.HBaseConstants.CF_PROPERTY_VALUE;
import static org.testng.Assert.assertEquals;

/**
 * Test class for {@link HBasePropReg}
 */
public class HBasePropRegTest {
  /**
   * Test the toHBaseFilter function
   */
  @Test
  public void testToHBaseFilter() {
    String key = "key";
    Pattern pattern = Pattern.compile("^FooBar.*$");

    HBasePropReg<EPGMVertex> vertexFilter = new HBasePropReg<>(key, pattern);

    FilterList expectedFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTY_VALUE),
      Bytes.toBytesBinary(key),
      CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(pattern.pattern()));

    // Define that the entire row will be skipped if the column is not found
    valueFilter.setFilterIfMissing(true);

    SingleColumnValueFilter typeFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTY_TYPE),
      Bytes.toBytesBinary(key),
      CompareFilter.CompareOp.EQUAL,
      new byte[] {Type.STRING.getTypeByte()});

    // Define that the entire row will be skipped if the column is not found
    typeFilter.setFilterIfMissing(true);

    expectedFilter.addFilter(typeFilter);
    expectedFilter.addFilter(valueFilter);

    assertEquals(vertexFilter.toHBaseFilter(false).toString(), expectedFilter.toString(),
      "Failed during filter comparison for key [" + key + "].");
  }
}
