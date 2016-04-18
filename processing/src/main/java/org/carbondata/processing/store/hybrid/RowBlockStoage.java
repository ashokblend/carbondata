package org.carbondata.processing.store.hybrid;

import java.util.concurrent.Callable;

import org.carbondata.core.datastorage.store.columnar.IndexStorage;

public class RowBlockStoage implements IndexStorage, Callable<IndexStorage> {

  private RowStoreDataHolder rowStoreDataHolder;
  public RowBlockStoage(DataHolder rowStoreDataHolder){
    this.rowStoreDataHolder=(RowStoreDataHolder)rowStoreDataHolder;
  }
  @Override
  public boolean isAlreadySorted() {
    return true;
  }

  @Override
  public RowStoreDataHolder getDataAfterComp() {
    //not required for Row store
    return null;
  }

  @Override
  public RowStoreDataHolder getIndexMap() {
    // not required for row store
    return null;
  }

  @Override
  public byte[][] getKeyBlock() {
    return rowStoreDataHolder.getData();
  }

  @Override
  public RowStoreDataHolder getDataIndexMap() {
    //not required for row store
    return null;
  }

  @Override
  public int getTotalSize() {
    return rowStoreDataHolder.getTotalSize();
  }
 // @Override
  public byte[] getMinMax() {
    //for each chunk min max
    byte[] minData=rowStoreDataHolder.getMin();
    byte[] maxData=rowStoreDataHolder.getMax();
    byte[] minMax=new byte[minData.length+maxData.length];
    System.arraycopy(minData, 0, minMax, 0, minData.length);
    System.arraycopy(maxData, 0, minMax, minData.length, maxData.length);
    return minMax;
  }
  @Override
  public IndexStorage call() throws Exception {
    return this;
  }
}
