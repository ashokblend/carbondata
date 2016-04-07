/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.carbon.datastore.block;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.leafnode.BlockMetadata;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.util.CarbonUtil;

/**
 * This class contains all the details about the restructuring information of the block.
 * This will be used during query execution to handle restructure information
 */
public class BlockRSInfo {

    /**
     * key generator of the block which was used to generate the mdkey
     */
    private KeyGenerator normalDimensionKeyGenerator;

    /**
     * key generator used for complex dimension
     */
    private KeyGenerator complexDimensionKeyGenerator;

    /**
     * list of dimension present in the block
     */
    private List<CarbonDimension> dimensions;

    /**
     * list of measure present in the block
     */
    private List<CarbonMeasure> measures;

    /**
     * cardinality of dimension columns participated in keygenerator
     */
    private int[] dimColumnCardinality;

    public BlockRSInfo(BlockMetadata blockMetadata) {
        dimensions = new ArrayList<CarbonDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        measures = new ArrayList<CarbonMeasure>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        fillDetailFromBlockMetadata(blockMetadata);
    }

    /**
     * below method will fill detail from block meta data
     *
     * @param blockMetadata
     */
    private void fillDetailFromBlockMetadata(BlockMetadata blockMetadata) {
        List<ColumnSchema> columnInTable = blockMetadata.getColumnInTable();
        ColumnSchema columnSchema = null;
        // ordinal will be required to read the data from file block
        int dimensonOrdinal = 0;
        int measureOrdinal = 0;
        // table ordinal is actually a schema ordinal this is required as cardinality array
        // which is stored in segment info contains -1 if that particular column is n
        int tableOrdinal = 0;
        // creating a list as we do not know how many dimension not participated in the mdkey
        List<Integer> cardinalityIndexForDimensionColumn = new ArrayList<Integer>(
                blockMetadata.getLeafNodeList().get(0).getDimensionColumnChunk().size());

        for (int i = 0; i < columnInTable.size(); i++) {
            columnSchema = columnInTable.get(i);
            if (columnSchema.isDimensionColumn()) {
                CarbonDimension carbonDimension =
                        new CarbonDimension(columnSchema, dimensonOrdinal++, tableOrdinal++);
                // not adding the cardinality of the non dictionary
                // column as it was not the part of mdkey
                if (!carbonDimension.getEncoder().contains(Encoding.DICTIONARY)) {
                    cardinalityIndexForDimensionColumn.add(tableOrdinal);
                }
                dimensions.add(carbonDimension);
            } else {
                measures.add(new CarbonMeasure(columnSchema, measureOrdinal++, tableOrdinal++));
            }
        }
        dimColumnCardinality = new int[cardinalityIndexForDimensionColumn.size()];
        int index = 0;
        // filling the cardinality of the dimension column to create the keygenerator
        for (Integer cardinalityArrayIndex : cardinalityIndexForDimensionColumn) {
            dimColumnCardinality[index++] =
                    blockMetadata.getSegmentInfo().getColumnCardinality()[cardinalityArrayIndex];
        }
        dimensions = CarbonUtil.arrangeDimension(dimensions);
        this.normalDimensionKeyGenerator =
                KeyGeneratorFactory.getKeyGenerator(dimColumnCardinality);
    }

    /**
     * @return the normalDimensionKeyGenerator
     */
    public KeyGenerator getNormalDimensionKeyGenerator() {
        return normalDimensionKeyGenerator;
    }

    /**
     * @return the complexDimensionKeyGenerator
     */
    public KeyGenerator getComplexDimensionKeyGenerator() {
        return complexDimensionKeyGenerator;
    }

    /**
     * @return the dimensions
     */
    public List<CarbonDimension> getDimensions() {
        return dimensions;
    }

    /**
     * @return the measures
     */
    public List<CarbonMeasure> getMeasures() {
        return measures;
    }

    /**
     * @return the dimColumnCardinality
     */
    public int[] getDimColumnCardinality() {
        return dimColumnCardinality;
    }
}
