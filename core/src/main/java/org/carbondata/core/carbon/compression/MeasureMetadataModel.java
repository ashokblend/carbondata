package org.carbondata.core.carbon.compression;

public class MeasureMetadataModel {

	private Object[] maxValue;
	
	private Object[] minValue;
	
	private int[] decimalLength;
	
	private char[] type;
	
	public MeasureMetadataModel(Object[] maxValue,Object[] minValue,int[] decimalLength,char[] type) {
		this.maxValue=maxValue;
		this.minValue=minValue;
		this.decimalLength=decimalLength;
		this.type=type;
	}

	/**
	 * @return the maxValue
	 */
	public Object[] getMaxValue() {
		return maxValue;
	}

	/**
	 * @param maxValue the maxValue to set
	 */
	public void setMaxValue(Object[] maxValue) {
		this.maxValue = maxValue;
	}

	/**
	 * @return the minValue
	 */
	public Object[] getMinValue() {
		return minValue;
	}

	/**
	 * @param minValue the minValue to set
	 */
	public void setMinValue(Object[] minValue) {
		this.minValue = minValue;
	}

	/**
	 * @return the decimalLength
	 */
	public int[] getDecimalLength() {
		return decimalLength;
	}

	/**
	 * @param decimalLength the decimalLength to set
	 */
	public void setDecimalLength(int[] decimalLength) {
		this.decimalLength = decimalLength;
	}

	/**
	 * @return the type
	 */
	public char[] getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(char[] type) {
		this.type = type;
	}
}
