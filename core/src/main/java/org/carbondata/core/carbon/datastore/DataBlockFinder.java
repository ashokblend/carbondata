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

/**
 * Below Interface is to search a block
 */
public interface DataBlockFinder {

    /**
     * Below method will be used to get the data block based on search key
     *
     * @param dataBlocks complete data blocks present
     * @param serachKey  key to be search
     * @param isFirst    in block we can have duplicate data if data is sorted then
     *                   for scanning we need to scan first instance of the search key till last
     *                   so for this is user is passing is first true it will return the first instance
     *                   if false then it will return the last. In case of unsorted data this parameter
     *                   does not matter implementation is will handle that scenario
     * @return data block
     */
    DataBlock findDataBlock(DataBlock dataBlocks, IndexKey serachKey, boolean isFirst);
}
