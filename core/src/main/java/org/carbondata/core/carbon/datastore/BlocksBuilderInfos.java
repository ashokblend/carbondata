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
package org.carbondata.core.carbon.datastore;

import java.util.List;

import org.carbondata.core.carbon.metadata.leafnode.DataFileMetadata;

/**
 * below class holds the meta data requires to build the blocks
 */
public class BlocksBuilderInfos {

    /**
     * holds all the information about data
     * file meta data
     */
    private List<DataFileMetadata> dataFileMetadataList;

    /**
     * size of the each column value size
     * this will be useful for reading
     */
    private int[] dimensionColumnValueSize;

    /**
     * block file path
     */
    private String filePath;

    /**
     * @return the eachDimensionBlockSize
     */
    public int[] getDimensionColumnValueSize() {
        return dimensionColumnValueSize;
    }

    /**
     * @param eachDimensionBlockSize the eachDimensionBlockSize to set
     */
    public void setEachDimensionBlockSize(int[] eachDimensionBlockSize) {
        this.dimensionColumnValueSize = eachDimensionBlockSize;
    }

    /**
     * @return the filePath
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * @param filePath the filePath to set
     */
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    /**
     * @return the dataFileMetadataList
     */
    public List<DataFileMetadata> getDataFileMetadataList() {
        return dataFileMetadataList;
    }

    /**
     * @param dataFileMetadataList the dataFileMetadataList to set
     */
    public void setDataFileMetadataList(List<DataFileMetadata> dataFileMetadataList) {
        this.dataFileMetadataList = dataFileMetadataList;
    }

    /**
     * @param dimensionColumnValueSize the dimensionColumnValueSize to set
     */
    public void setDimensionColumnValueSize(int[] dimensionColumnValueSize) {
        this.dimensionColumnValueSize = dimensionColumnValueSize;
    }
}
