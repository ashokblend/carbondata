package org.carbondata.processing.store.hybrid;

import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.vo.HybridStoreModel;

/**
 * This will hold row store data. These data will be available in chunks. Size each chunk is
 * configurable. It will also hold min,max value of each chunk.
 * @author A00902717
 */
public class RowStoreDataHolder implements DataHolder {



  private int noOfRecords;

  /**
   * rowsData[row no][data] row chunk: complete rows data is split based on total no of
   * records: row index data: data of each row
   */
  private byte[][] rowStoreData;
  
  /**
   * This will have min max value of each chunk
   */
  private RowStoreMinMax rowStoreMinMax;



  /**
   * each row size in bytes
   */
  private int keyBlockSize;


  public RowStoreDataHolder(HybridStoreModel hybridStoreModel, ColumnarSplitter columnarSplitter,int colGroup,
      int noOfRecords) {
    this.noOfRecords = noOfRecords;
    this.keyBlockSize=columnarSplitter.getBlockKeySize()[colGroup];
    this.rowStoreMinMax = new RowStoreMinMax(hybridStoreModel, columnarSplitter,colGroup);
    rowStoreData = new byte[noOfRecords][];
  }

  public void addData(byte[] rowsData, int rowIndex) {
    rowStoreData[rowIndex] = rowsData;
    rowStoreMinMax.add(rowsData);
  }

  /**
   * this will return min of each chunk
   * @return
   */
  public byte[] getMin() {
   return rowStoreMinMax.getMax();
  }

  /**
   * this will return max of each chunk
   * @return
   */
  public byte[] getMax() {
    return rowStoreMinMax.getMax();
  }


  public int getKeyBlockSize() {
    return keyBlockSize;
  }


  @Override
  public byte[][] getData() {
    return rowStoreData;
  }

  @Override
  public byte[][] getMinMax() {
    // TODO Auto-generated method stub
    return null;
  }
  
  public int getTotalSize(){
    return noOfRecords*keyBlockSize;
  }

}
