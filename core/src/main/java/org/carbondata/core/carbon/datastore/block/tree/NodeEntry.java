package org.carbondata.core.carbon.datastore.block.tree;

public class NodeEntry {
	
	/**
	 * key which is generated from key generator
	 */
	private byte[] dictionaryKeys;
	
	/**
	 * dictionary which was no generated using key generator
	 *  
	 */
	private byte[] noDictionaryKeys;

	/**
	 * @return the dictionaryKeys
	 */
	public byte[] getDictionaryKeys() {
		return dictionaryKeys;
	}

	/**
	 * @param dictionaryKeys the dictionaryKeys to set
	 */
	public void setDictionaryKeys(byte[] dictionaryKeys) {
		this.dictionaryKeys = dictionaryKeys;
	}

	/**
	 * @return the noDictionaryKeys
	 */
	public byte[] getNoDictionaryKeys() {
		return noDictionaryKeys;
	}

	/**
	 * @param noDictionaryKeys the noDictionaryKeys to set
	 */
	public void setNoDictionaryKeys(byte[] noDictionaryKeys) {
		this.noDictionaryKeys = noDictionaryKeys;
	}
}
