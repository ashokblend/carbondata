package org.carbondata.core.carbon.datastore.block.tree.impl;

import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.block.tree.DataStore;
import org.carbondata.core.carbon.datastore.block.tree.DataStoreBlock;
import org.carbondata.core.carbon.metadata.index.LeafNodeIndex;
import org.carbondata.core.carbon.metadata.leafnode.LeafNodeInfo;

public class BTree implements DataStore {

	/**
	 * Attribute for Carbon LOGGER
	 */
	private static final LogService LOGGER = LogServiceFactory
			.getLogService(BTree.class.getName());

	/**
	 * Maximum number of entries in leaf nodes
	 */
	private int maxNumberOfEntriesInLeaf;

	/**
	 * Maximum number of entries in intermediate nodes
	 */
	private int maxNumberOfEntriesInInter;

	/**
	 * Number of leaf nodes
	 */
	private int nLeaf;

	/**
	 * is file based store
	 */
	private boolean isFileStore;

	public BTree() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void buildTree(List<LeafNodeInfo> leafNodes,
			List<LeafNodeIndex> leafNodeIndexes, boolean keepDataPageDetail) {
		// TODO Auto-generated method stub

	}

	@Override
	public DataStoreBlock getDataStoreBlock(byte[] key, boolean isFirst) {
		// TODO Auto-generated method stub
		return null;
	}

}
