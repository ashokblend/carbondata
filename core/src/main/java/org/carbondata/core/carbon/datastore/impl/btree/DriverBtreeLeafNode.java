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
package org.carbondata.core.carbon.datastore.impl.btree;

import org.carbondata.core.carbon.datastore.BlocksBuilderInfos;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeMinMaxIndex;

/**
 * Leaf node for btree where only min max will be store 
 * this can be used from driver when only we need to find 
 * whether particular block be selected for query execution
 *
 */
public class DriverBtreeLeafNode extends AbstractBtreeLeafNode{

	/**
	 * Create a leaf node 
	 * @param builderInfos
	 * 			builder infos which have required metadata to create a leaf node 
	 * @param leafIndex
	 * 			leaf node index
	 * @param metadataIndex
	 * 			metadata index
	 */
	public DriverBtreeLeafNode(BlocksBuilderInfos builderInfos, int metadataIndex, long nodeNumber) {
		LeafNodeMinMaxIndex minMaxIndex = builderInfos
				.getDataFileMetadataList().get(metadataIndex)
				.getLeafNodeIndex().getMinMaxIndex();
		maxKeyOfColumns = minMaxIndex.getMaxValues();
		minKeyOfColumns = minMaxIndex.getMinValues();
		numberOfKeys =1;
		this.nodeNumber=nodeNumber;
	}
}
