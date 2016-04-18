package org.carbondata.processing.store.hybrid;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.core.vo.HybridStoreModel;

/**
 * This will store give min max of rows data
 *
 */
public class RowStoreMinMax {
  private HybridStoreModel hybridStoreModel;
  private KeyGenerator keyGenerator;
  private byte[][] min;
  private byte[][] max;
  private int noOfCol;
  private int colGroup;
  private int[][] maskBytePosition;
  private int[][] maskByteRange;
  private byte[][] maxKeys;
  
  public RowStoreMinMax(HybridStoreModel hybridStoreModel,ColumnarSplitter columnarSplitter, int colGroup){
    this.hybridStoreModel=hybridStoreModel;
    this.keyGenerator=(KeyGenerator)columnarSplitter;
    this.colGroup=colGroup;
    this.noOfCol=hybridStoreModel.getColumnSplit()[colGroup];
    min=new byte[noOfCol][];
    max=new byte[noOfCol][];
    initialise();
    
  }
  private void initialise() {
    try{
      maskBytePosition=new int[noOfCol][];
      maskByteRange=new int[noOfCol][];
      maxKeys=new byte[noOfCol][];
      for(int i=0;i<noOfCol;i++){
        maskByteRange[i]=getMaskByteRange(hybridStoreModel.getColumnGroup()[colGroup][i]);
        maskBytePosition[i]=new int[keyGenerator.getKeySizeInBytes()];
        updateMaskedKeyRanges(maskBytePosition[i], maskByteRange[i]);
        //generating maxkey
        long[] maxKey=new long[keyGenerator.getKeySizeInBytes()];
        maxKey[hybridStoreModel.getColumnGroup()[colGroup][i]]=Long.MAX_VALUE;
        maxKeys[i]=keyGenerator.generateKey(maxKey);
      }  
    }catch(Exception e){
      e.printStackTrace();
    }
    
    
  }
  private int[] getMaskByteRange(int col) {
    Set<Integer> integers=new HashSet<>();
    int[] range=keyGenerator.getKeyByteOffsets(col);
    for (int j = range[0]; j <= range[1]; j++) {
      integers.add(j);
    }
    int[] byteIndexs = new int[integers.size()];
    int j = 0;
    for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
        Integer integer = (Integer) iterator.next();
        byteIndexs[j++] = integer.intValue();
    }
    return byteIndexs;
  }
  
  public static void updateMaskedKeyRanges(int[] maskedKey, int[] maskedKeyRanges) {
    Arrays.fill(maskedKey, -1);
    for (int i = 0; i < maskedKeyRanges.length; i++) {
        maskedKey[maskedKeyRanges[i]] = i;
    }
  }
  /**
   * Below method will be used to get the masked key
   *
   * @param data
   * @return maskedKey
   */
  private static byte[] getMaskedKey(byte[] data,int[] maskByteRange,byte[] maxKey) {
      int keySize=maskByteRange.length;
      byte[] maskedKey = new byte[keySize];
      int counter = 0;
      int byteRange = 0;
      for (int i = 0; i < keySize; i++) {
          byteRange = maskByteRange[i];
          maskedKey[counter++] = (byte) (data[byteRange] & maxKey[byteRange]);
      }
      return maskedKey;
  }
  public void add(byte[] rowStoreData){
    try{
      for(int i=0;i<noOfCol;i++){
        long[] keyArray=keyGenerator.getKeyArray(rowStoreData,maskBytePosition[i]);
        byte[] data=keyGenerator.generateKey(keyArray);
        byte[] col=getMaskedKey(data,maskByteRange[i], maxKeys[i]);
        setMin(col,i);
        setMax(col,i);  
      }
        
    }catch(Exception e){
     e.printStackTrace(); 
    }
   /* int startIndex=hybridStoreModel.getColumnGroup()[colGroup][0];
    int position=0;
    for(int i=0;i<noOfCol;i++){
      int[] range=keyGenerator.getKeyByteOffsets(startIndex++);
      byte[] col=new byte[range[1]-range[0]+1];
      System.arraycopy(rowStoreData, position, col, 0, col.length);
      position+=range[1]-range[0];
      setMin(col,i);
      setMax(col,i);
    }*/
  }
  private void setMax(byte[] col, int index) {

    if(null==min[index]){
      min[index]=col;
    }else{
      if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(col,min[index])<0){
        min[index]=col;
      }
    }
  }
  private void setMin(byte[] col, int index) {
    if(null==max[index]){
      max[index]=col;
    }else {
      if(ByteUtil.UnsafeComparer.INSTANCE.compareTo(col,max[index])>0){
        max[index]=col;
      }
        
    }
  }
  
  public byte[] getMax(){
    int size=0;
    for(int i=0;i<noOfCol;i++){
      size+=min[i].length;
    }
    ByteBuffer bb=ByteBuffer.allocate(size);
    for(int i=0;i<noOfCol;i++){
      bb.put(min[i]);
    }
    return bb.array();
  }
  
  public byte[] getMin(){
    int size=0;
    for(int i=0;i<noOfCol;i++){
      size+=max[i].length;
    }
    ByteBuffer bb=ByteBuffer.allocate(size);
    for(int i=0;i<noOfCol;i++){
      bb.put(max[i]);
    }
    return bb.array();
  }
  
  public boolean isHybridStore(){
    return hybridStoreModel.isHybridStore();
  }

}
