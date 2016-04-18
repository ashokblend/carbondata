package org.carbondata.processing.store.hybrid;

public class ColumDataHolder implements DataHolder {

  private byte[][] data;
  public ColumDataHolder(int noOfRow) {
    data=new byte[noOfRow][];
  }

  @Override
  public void addData(byte[] rowRecord,int rowIndex) {
   data[rowIndex]=rowRecord;
  }

  @Override
  public byte[][] getData() {
    return data;
  }

  @Override
  public byte[][] getMinMax() {
    // TODO Auto-generated method stub
    return null;
  }

}
