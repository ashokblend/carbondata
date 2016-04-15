package org.carbondata.core.carbon.datastore.impl.btree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.carbondata.core.carbon.datastore.BlocksBuilder;
import org.carbondata.core.carbon.datastore.BlocksBuilderInfos;
import org.carbondata.core.carbon.datastore.DataBlock;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.DataBlockFinder;
import org.carbondata.core.carbon.metadata.leafnode.DataFileMetadata;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeBtreeIndex;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeIndex;
import org.carbondata.core.carbon.metadata.leafnode.indexes.LeafNodeMinMaxIndex;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.carbondata.core.util.CarbonUtil;
import org.junit.Test;

public class SegmentBtreeFormatBuilderTest extends TestCase {


	
	@Test
	public void testBtreeBuldingIsPorper()
	{
		BlocksBuilder builder =new DriverBtreeFormatBuilder();
		List<DataFileMetadata> dataFileMetadataList = getDataFileMetadataList();
		BlocksBuilderInfos infos = new BlocksBuilderInfos();
		infos.setDataFileMetadataList(dataFileMetadataList);
		builder.build(infos);
		
	}

	@Test
	public void testBtreeBuilderGetMethodIsGivingNotNullRootNode()
	{
		BlocksBuilder builder =new DriverBtreeFormatBuilder();
		List<DataFileMetadata> dataFileMetadataList = getDataFileMetadataList();
		BlocksBuilderInfos infos = new BlocksBuilderInfos();
		infos.setDataFileMetadataList(dataFileMetadataList);
		builder.build(infos);
		DataBlock dataBlock = builder.get();
		assertTrue(dataBlock!=null);
	}
	
	@Test
	/**
	 * This method will be used to test when proper no dictionary key is passed 
	 * search is giving proper leaf node which is 2
	 */
	public void testBtreeSerachIsWorkingAndGivingPorperLeafNodeWithNoDictionary1()
	{
		BlocksBuilder builder = new DriverBtreeFormatBuilder();
		List<DataFileMetadata> dataFileMetadataList = getFileMetadataListWithOnlyNoDictionaryKey();
		BlocksBuilderInfos infos = new BlocksBuilderInfos();
		infos.setDataFileMetadataList(dataFileMetadataList);
		builder.build(infos);
		DataBlock dataBlock = builder.get();
		assertTrue(dataBlock != null);
		DataBlockFinder finder = new BTreeBasedBlockFinder(new int[] { -1 });
		IndexKey key = new IndexKey();
		ByteBuffer buffer = ByteBuffer.allocate(4 + 1);
		buffer.rewind();
		buffer.put((byte) 1);
		buffer.putInt(12);
		buffer.array();
		key.setNoDictionaryKeys(buffer.array());
		DataBlock findFirstBlock = finder.findDataBlock(dataBlock, key, true);
		assertEquals(1, findFirstBlock.getBlockNumber());
		DataBlock findLastBlock = finder.findDataBlock(dataBlock, key, true);
		assertEquals(1, findLastBlock.getBlockNumber());
	}
	
	@Test
	/**
	 * Below method will test when key which is not present and key which is less than 
	 * first node key is passes for searching it should give first block
	 */
	public void testBtreeSerachIsWorkingAndGivingPorperLeafNodeWithNoDictionary()
	{
		BlocksBuilder builder = new DriverBtreeFormatBuilder();
		List<DataFileMetadata> dataFileMetadataList = getFileMetadataListWithOnlyNoDictionaryKey();
		BlocksBuilderInfos infos = new BlocksBuilderInfos();
		infos.setDataFileMetadataList(dataFileMetadataList);
		builder.build(infos);
		DataBlock dataBlock = builder.get();
		assertTrue(dataBlock != null);
		DataBlockFinder finder = new BTreeBasedBlockFinder(new int[] { -1 });
		IndexKey key = new IndexKey();
		ByteBuffer buffer = ByteBuffer.allocate(4 + 1);
		buffer.rewind();
		buffer.put((byte) 1);
		buffer.putInt(0);
		buffer.array();
		key.setNoDictionaryKeys(buffer.array());
		DataBlock findFirstBlock = finder.findDataBlock(dataBlock, key, true);
		assertEquals(0, findFirstBlock.getBlockNumber());
		DataBlock findLastBlock = finder.findDataBlock(dataBlock, key, false);
		assertEquals(0, findLastBlock.getBlockNumber());
	}
	
	
	private List<DataFileMetadata> getDataFileMetadataList() {
		List<DataFileMetadata> list = new ArrayList<DataFileMetadata>();
		try {
			int[] dimensionBitLength = CarbonUtil.getDimensionBitLength(
					new int[] { 10000, 10000 }, new int[] { 1, 1 });
			KeyGenerator multiDimKeyVarLengthGenerator = new MultiDimKeyVarLengthGenerator(
					dimensionBitLength);
			int i = 1;
			while (i < 1001) {
				byte[] startKey = multiDimKeyVarLengthGenerator
						.generateKey(new int[] { i, i });
				byte[] endKey = multiDimKeyVarLengthGenerator
						.generateKey(new int[] { i + 10, i + 10 });
				ByteBuffer buffer = ByteBuffer.allocate(4+1);
				buffer.rewind();
				buffer.put((byte)1);
				buffer.putInt(i);
				buffer.array();
				byte[] noDictionaryStartKey =buffer.array();
				
				ByteBuffer buffer1 = ByteBuffer.allocate(4+1);
				buffer1.rewind();
				buffer1.put((byte)1);
				buffer1.putInt(i+10);
				buffer1.array();
				byte[] noDictionaryEndKey = buffer.array();
				DataFileMetadata fileMetadata = getFileMetadata(startKey,
						endKey, noDictionaryStartKey, noDictionaryEndKey);
				list.add(fileMetadata);
				i = i + 10;
			}
		} catch (Exception e) {

		}
		return list;
	}

	
	private List<DataFileMetadata> getFileMetadataListWithOnlyNoDictionaryKey() {
		List<DataFileMetadata> list = new ArrayList<DataFileMetadata>();
		try {
			int[] dimensionBitLength = CarbonUtil.getDimensionBitLength(
					new int[] { 10000, 10000 }, new int[] { 1, 1 });
			KeyGenerator multiDimKeyVarLengthGenerator = new MultiDimKeyVarLengthGenerator(
					dimensionBitLength);
			int i = 1;
			while (i < 1001) {
				byte[] startKey = multiDimKeyVarLengthGenerator
						.generateKey(new int[] { i, i });
				byte[] endKey = multiDimKeyVarLengthGenerator
						.generateKey(new int[] { i + 10, i + 10 });
				ByteBuffer buffer = ByteBuffer.allocate(1+4);
				buffer.rewind();
				buffer.put((byte)1);
				buffer.putInt(i);
				buffer.array();
				byte[] noDictionaryStartKey =buffer.array();
				
				ByteBuffer buffer1 = ByteBuffer.allocate(1+4);
				buffer1.rewind();
				buffer1.put((byte)1);
				buffer1.putInt(i+10);
				buffer1.array();
				byte[] noDictionaryEndKey = buffer.array();
				DataFileMetadata fileMetadata = getFileMatadataWithOnlyNoDictionaryKey(startKey,
						endKey, noDictionaryStartKey, noDictionaryEndKey);
				list.add(fileMetadata);
				i = i + 10;
			}
		} catch (Exception e) {

		}
		return list;
	}

	private DataFileMetadata getFileMetadata(byte[] startKey, byte[] endKey,
			byte[] noDictionaryStartKey, byte[] noDictionaryEndKey) {
		DataFileMetadata dataFileMetadata = new DataFileMetadata();
		LeafNodeIndex index = new LeafNodeIndex();
		LeafNodeBtreeIndex btreeIndex = new LeafNodeBtreeIndex();
		ByteBuffer buffer = ByteBuffer.allocate(4 + startKey.length + 4
				+ noDictionaryStartKey.length);
		buffer.putInt(startKey.length);
		buffer.putInt(noDictionaryStartKey.length);
		buffer.put(startKey);
		buffer.put(noDictionaryStartKey);
		buffer.rewind();
		btreeIndex.setStartKey(buffer.array());
		ByteBuffer buffer1 = ByteBuffer.allocate(4 + startKey.length + 4
				+ noDictionaryEndKey.length);
		buffer1.putInt(endKey.length);
		buffer1.putInt(noDictionaryEndKey.length);
		buffer1.put(endKey);
		buffer1.put(noDictionaryEndKey);
		buffer1.rewind();
		btreeIndex.setEndKey(buffer1.array());
		LeafNodeMinMaxIndex minMax = new LeafNodeMinMaxIndex();
		minMax.setMaxValues(new byte[][] { endKey, noDictionaryEndKey });
		minMax.setMinValues(new byte[][] { startKey, noDictionaryStartKey });
		index.setBtreeIndex(btreeIndex);
		index.setMinMaxIndex(minMax);
		dataFileMetadata.setLeafNodeIndex(index);
		return dataFileMetadata;
	}
	
	private DataFileMetadata getFileMatadataWithOnlyNoDictionaryKey(byte[] startKey, byte[] endKey,
			byte[] noDictionaryStartKey, byte[] noDictionaryEndKey) {
		DataFileMetadata dataFileMetadata = new DataFileMetadata();
		LeafNodeIndex index = new LeafNodeIndex();
		LeafNodeBtreeIndex btreeIndex = new LeafNodeBtreeIndex();
		ByteBuffer buffer = ByteBuffer.allocate(4 + 0 + 4
				+ noDictionaryStartKey.length);
		buffer.putInt(0);
		buffer.putInt(noDictionaryStartKey.length);
		buffer.put(noDictionaryStartKey);
		buffer.rewind();
		btreeIndex.setStartKey(buffer.array());
		ByteBuffer buffer1 = ByteBuffer.allocate(4 + 0 + 4
				+ noDictionaryEndKey.length);
		buffer1.putInt(0);
		buffer1.putInt(noDictionaryEndKey.length);
		buffer1.put(noDictionaryEndKey);
		buffer1.rewind();
		btreeIndex.setEndKey(buffer1.array());
		LeafNodeMinMaxIndex minMax = new LeafNodeMinMaxIndex();
		minMax.setMaxValues(new byte[][] { endKey, noDictionaryEndKey });
		minMax.setMinValues(new byte[][] { startKey, noDictionaryStartKey });
		index.setBtreeIndex(btreeIndex);
		index.setMinMaxIndex(minMax);
		dataFileMetadata.setLeafNodeIndex(index);
		return dataFileMetadata;
	}

}
