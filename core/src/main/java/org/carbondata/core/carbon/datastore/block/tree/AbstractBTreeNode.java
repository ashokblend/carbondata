package org.carbondata.core.carbon.datastore.block.tree;


public abstract class AbstractBTreeNode implements DataStoreBlock{

	/**
	 * to check whether node in a btree is a leaf node ot not
	 * @return leaf node or not
	 */
	public abstract boolean isLeafNode();
	
	/**
	 * below method will return the next node in a linked list
	 * 
	 */
    public abstract AbstractBTreeNode getNext();
}
