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
package org.carbondata.core.carbon.datastore.block.store.dimension;

/**
 * Below class is hold the meta data of the data
 */
public class DimensionDataHolderMetadata {

    /**
     * inverted index of the data
     */
    private int[] invertedIndexes;

    /**
     * reverse index of the data
     */
    private int[] invertedIndexesReverse;

    /**
     * rle index block
     */
    private int[] rleIndexes;

    /**
     * is uncompressed
     */
    private boolean isUnCompressed;

    /**
     * each row size
     */
    private short eachRowSize;

    /**
     * @return the invertedIndexes
     */
    public int[] getInvertedIndexes() {
        return invertedIndexes;
    }

    /**
     * @param invertedIndexes the invertedIndexes to set
     */
    public void setInvertedIndexes(int[] invertedIndexes) {
        this.invertedIndexes = invertedIndexes;
    }

    /**
     * @return the invertedIndexesReverse
     */
    public int[] getInvertedIndexesReverse() {
        return invertedIndexesReverse;
    }

    /**
     * @param invertedIndexesReverse the invertedIndexesReverse to set
     */
    public void setInvertedIndexesReverse(int[] invertedIndexesReverse) {
        this.invertedIndexesReverse = invertedIndexesReverse;
    }

    /**
     * @return the rleIndexes
     */
    public int[] getRleIndexes() {
        return rleIndexes;
    }

    /**
     * @param rleIndexes the rleIndexes to set
     */
    public void setRleIndexes(int[] rleIndexes) {
        this.rleIndexes = rleIndexes;
    }

    /**
     * @return the isUnCompressed
     */
    public boolean isUnCompressed() {
        return isUnCompressed;
    }

    /**
     * @param isUnCompressed the isUnCompressed to set
     */
    public void setUnCompressed(boolean isUnCompressed) {
        this.isUnCompressed = isUnCompressed;
    }

    /**
     * @return the eachRowSize
     */
    public short getEachRowSize() {
        return eachRowSize;
    }

    /**
     * @param eachRowSize the eachRowSize to set
     */
    public void setEachRowSize(short eachRowSize) {
        this.eachRowSize = eachRowSize;
    }
}
