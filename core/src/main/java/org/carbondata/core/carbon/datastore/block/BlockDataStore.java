package org.carbondata.core.carbon.datastore.block;

import java.util.List;

import org.carbondata.core.carbon.metadata.leafnode.BlockMetadata;
import org.carbondata.core.carbon.metadata.leafnode.LeafNodeInfo;

/**
 * below class is responsible for loading the btree and it will have all the metadata
 * related to Btree 
 *
 */
public class BlockDataStore {

	private long numberOfKeys;
	
	/**
	 * Below method load the btree 
	 * @param blockMetadata
	 */
	public void loadBtree(BlockMetadata blockMetadata)
	{
		List<LeafNodeInfo> leafNodeList = blockMetadata.getLeafNodeList();
	}
}
