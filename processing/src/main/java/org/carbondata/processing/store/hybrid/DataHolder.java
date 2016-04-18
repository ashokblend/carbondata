package org.carbondata.processing.store.hybrid;

public interface DataHolder {

  public void addData(byte [] rowRecord,int rowIndex);
  public byte[][] getData();
  
  public byte[][] getMinMax();
}
