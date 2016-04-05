package org.carbondata.core.carbon.datastore.block.tree;

import java.util.List;

import org.carbondata.core.carbon.metadata.index.LeafNodeIndex;
import org.carbondata.core.carbon.metadata.leafnode.LeafNodeInfo;

public interface DataStore {

	/**
	 * Below method will be used to build the btree
	 * @param leafNodes
	 * 			leaf node details
	 * @param leafNodeIndexes
	 * 			leaf node indexes
	 */
	void buildTree(List<LeafNodeInfo> leafNodes,List<LeafNodeIndex> leafNodeIndexes,boolean keepDataPageDetail);
	
	/**
	 * Get the block based on the key
	 * @param key
	 * 			
	 * @param isFirst
	 * 			as in data block can have duplicate block so to 
	 * get the block range we need to get the first entry and last 
	 * entry of the key range
	 * @return  leaf block
	 */
	DataStoreBlock getDataStoreBlock(byte[] key,boolean isFirst);
	
}
