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
package org.carbondata.core.carbon.datastore.block.tree.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.block.tree.BTreeNode;
import org.carbondata.core.carbon.datastore.block.tree.DataStore;
import org.carbondata.core.carbon.datastore.block.tree.LeafBlock;
import org.carbondata.core.carbon.datastore.block.tree.NodeEntry;
import org.carbondata.core.carbon.metadata.leafnode.BlockMetadata;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.util.CarbonEngineLogEvent;

public class BTree implements DataStore {

    /**
     * Attribute for Carbon LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(BTree.class.getName());

    /**
     * default Number of keys per page
     */
    private static final int DEFAULT_NUMBER_OF_ENTRIES_NONLEAF = 32;

    /**
     * Maximum number of entries in intermediate nodes
     */
    private int maxNumberOfEntriesInNonLeafNodes;

    /**
     * Number of leaf nodes
     */
    private int nLeaf;

    /**
     * root node of a btree
     */
    private BTreeNode root;

    /**
     * size of each dimension column block
     */
    private short[] eachBlockSize;

    /**
     * total number of tuples in btree
     */
    private long totalNumberOfTuples;

    public BTree(short[] eachBlockSize) {
        this.eachBlockSize = eachBlockSize;
        maxNumberOfEntriesInNonLeafNodes = Integer.parseInt(CarbonProperties.getInstance()
                .getProperty("com.huawei.datastore.internalnodesize",
                        DEFAULT_NUMBER_OF_ENTRIES_NONLEAF + ""));
    }

    /**
     * @param key
     * @param node
     * @return
     */
    private BTreeNode findFirstLeafNode(NodeEntry key, BTreeNode node) {
        int childNodeIndex;
        int low = 0;
        int high = node.blockSize() - 1;
        int mid = 0;
        int compareRes = -1;
        NodeEntry[] nodeKeys = node.getNodeKeys();
        //
        while (low <= high) {
            mid = (low + high) >>> 1;
            compareNodeEntries(key, nodeKeys[mid]);
            if (compareRes < 0) {
                high = mid - 1;
            } else if (compareRes > 0) {
                low = low + 1;
            } else {
                int currentPos = mid;
                while (currentPos - 1 >= 0 && compareNodeEntries(key, nodeKeys[mid]) == 0) {
                    currentPos--;
                }
                mid = currentPos;
                break;
            }
        }
        if (compareRes < 0) {
            if (mid > 0) {
                mid--;
            }
            childNodeIndex = mid;
        } else {
            childNodeIndex = mid;
        }
        node = node.getChild(childNodeIndex);
        return node;
    }

    /**
     * @param key
     * @param node
     * @return
     */
    private BTreeNode findLastLeafNode(NodeEntry key, BTreeNode node) {
        int childNodeIndex;
        int low = 0;
        int high = node.blockSize() - 1;
        int mid = 0;
        int compareRes = -1;
        NodeEntry[] nodeKeys = node.getNodeKeys();
        //
        while (low <= high) {
            mid = (low + high) >>> 1;
            compareNodeEntries(key, nodeKeys[mid]);
            if (compareRes < 0) {
                high = mid - 1;
            } else if (compareRes > 0) {
                low = low + 1;
            } else {
                int currentPos = mid;
                while (currentPos + 1 < node.blockSize()
                        && compareNodeEntries(key, nodeKeys[mid]) == 0) {
                    currentPos++;
                }
                mid = currentPos;
                break;
            }
        }
        if (compareRes < 0) {
            if (mid > 0) {
                mid--;
            }
            childNodeIndex = mid;
        } else {
            childNodeIndex = mid;
        }
        node = node.getChild(childNodeIndex);
        return node;
    }

    private int compareNodeEntries(NodeEntry first, NodeEntry second) {
        return 0;
    }

    @Override public void buildTree(BlockMetadata blockMetadata, boolean keepDataPageDetail,
            short[] eachDimensionBlockSize, String filePath) {
        long num = 0;
        int groupCounter;
        int nInternal = 0;
        FileHolder fileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath));
        BTreeNode curNode = null;
        BTreeNode prevNode = null;
        List<BTreeNode[]> nodeGroups =
                new ArrayList<BTreeNode[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        BTreeNode[] currentGroup = null;
        List<List<NodeEntry>> interNSKeyList =
                new ArrayList<List<NodeEntry>>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        List<NodeEntry> leafNSKeyList = null;
        for (int index = 0; index < blockMetadata.getLeafNodeList().size(); index++) {
            curNode = new BTreeLeafNode(blockMetadata, fileReader, index, filePath, eachBlockSize);
            num += blockMetadata.getLeafNodeList().get(index).getNumberOfRows();
            nLeaf++;
            if (prevNode != null) {
                prevNode.setNextNode(curNode);
            }
            prevNode = curNode;

            groupCounter = (nLeaf - 1) % (maxNumberOfEntriesInNonLeafNodes);
            if (groupCounter == 0) {
                // Create new node group if current group is full
                leafNSKeyList = new ArrayList<NodeEntry>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
                currentGroup = new BTreeNode[maxNumberOfEntriesInNonLeafNodes];
                nodeGroups.add(currentGroup);
                nInternal++;
                interNSKeyList.add(leafNSKeyList);
            }
            if (null != leafNSKeyList) {
                leafNSKeyList.add(convertStartKeyToNodeEntry(
                        blockMetadata.getLeafNodeIndex().getBtreeIndexList().get(index)
                                .getStartKey()));
            }
            if (null != currentGroup) {
                currentGroup[groupCounter] = curNode;
            }
        }
        if (num == 0) {
            curNode = new BTreeNonLeafNode();
            return;
        }
        addIntermediateNode(curNode, nodeGroups, currentGroup, interNSKeyList, nInternal);
        totalNumberOfTuples = num;
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "**********************************************"
                        + "***********Total Number Rows In BTREE: "
                        + totalNumberOfTuples);
        if (null != fileReader) {
            fileReader.finish();
        }
    }

    private NodeEntry convertStartKeyToNodeEntry(byte[] startKey) {
        NodeEntry entry = new NodeEntry();
        ByteBuffer buffer = ByteBuffer.wrap(startKey);
        buffer.rewind();
        int dictonaryKeySize = buffer.getInt();
        byte[] dictionaryKey = new byte[dictonaryKeySize];
        int nonDictonaryKeySize = buffer.getInt();
        byte[] nonDictionaryKey = new byte[nonDictonaryKeySize];
        buffer.get(dictionaryKey);
        buffer.get(nonDictionaryKey);
        entry.setDictionaryKeys(dictionaryKey);
        entry.setNoDictionaryKeys(nonDictionaryKey);
        return entry;
    }

    private void addIntermediateNode(BTreeNode curNode, List<BTreeNode[]> childNodeGroups,
            BTreeNode[] currentGroup, List<List<NodeEntry>> interNSKeyList,
            int numberOfInternalNode) {

        int groupCounter;
        // Build internal nodes level by level. Each upper node can have
        // upperMaxEntry keys and upperMaxEntry+1 children
        int remainder;
        int nHigh = numberOfInternalNode;
        boolean bRootBuilt = false;
        remainder = nLeaf % (maxNumberOfEntriesInNonLeafNodes);
        List<NodeEntry> interNSKeys = null;
        while (nHigh > 1 || !bRootBuilt) {
            List<BTreeNode[]> internalNodeGroups =
                    new ArrayList<BTreeNode[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
            List<List<NodeEntry>> interNSKeyTmpList =
                    new ArrayList<List<NodeEntry>>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
            numberOfInternalNode = 0;
            for (int i = 0; i < nHigh; i++) {
                // Create a new internal node
                curNode = new BTreeNonLeafNode();
                // Allocate a new node group if current node group is full
                groupCounter = i % (maxNumberOfEntriesInNonLeafNodes);
                if (groupCounter == 0) {
                    // Create new node group
                    currentGroup = new BTreeNonLeafNode[maxNumberOfEntriesInNonLeafNodes];
                    internalNodeGroups.add(currentGroup);
                    numberOfInternalNode++;
                    interNSKeys = new ArrayList<NodeEntry>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
                    interNSKeyTmpList.add(interNSKeys);
                }

                // Add the new internal node to current group
                if (null != currentGroup) {
                    currentGroup[groupCounter] = curNode;
                }
                int nNodes;

                if (i == nHigh - 1 && remainder != 0) {
                    nNodes = remainder;
                } else {
                    nNodes = maxNumberOfEntriesInNonLeafNodes;
                }
                // Point the internal node to its children node group
                curNode.setChildren(childNodeGroups.get(i));
                // Fill the internal node with keys based on its child nodes
                for (int j = 0; j < nNodes; j++) {
                    curNode.setKey(interNSKeyList.get(i).get(j));
                    if (j == 0 && null != interNSKeys) {
                        interNSKeys.add(interNSKeyList.get(i).get(j));

                    }
                }
            }
            // If nHigh is 1, we have the root node
            if (nHigh == 1) {
                bRootBuilt = true;
            }

            remainder = nHigh % (maxNumberOfEntriesInNonLeafNodes);
            nHigh = numberOfInternalNode;
            childNodeGroups = internalNodeGroups;
            interNSKeyList = interNSKeyTmpList;
        }
        root = curNode;
    }

    @Override public LeafBlock getDataStoreBlock(NodeEntry key, boolean isFirst) {
        BTreeNode node = root;
        if (isFirst) {
            while (!node.isLeafNode()) {
                node = findFirstLeafNode(key, node);
            }
        } else {
            while (!node.isLeafNode()) {
                node = findLastLeafNode(key, node);
            }
        }
        return node;
    }

    @Override public long numberOfTuplesInBTree() {
        return totalNumberOfTuples;
    }
}
