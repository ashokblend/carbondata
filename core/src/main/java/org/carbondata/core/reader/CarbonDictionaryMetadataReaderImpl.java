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

package org.carbondata.core.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.format.ColumnDictionaryChunkMeta;

import org.apache.thrift.TBase;

/**
 * This class perform the functionality of reading the dictionary metadata file
 */
public class CarbonDictionaryMetadataReaderImpl implements CarbonDictionaryMetadataReader {

  /**
   * carbon table identifier
   */
  private CarbonTableIdentifier carbonTableIdentifier;

  /**
   * HDFS store path
   */
  private String hdfsStorePath;

  /**
   * column name
   */
  private String columnIdentifier;

  /**
   * dictionary metadata file path
   */
  private String columnDictionaryMetadataFilePath;

  /**
   * dictionary metadata thrift file reader
   */
  private ThriftReader dictionaryMetadataFileReader;

  /**
   * Constructor
   *
   * @param hdfsStorePath         HDFS store path
   * @param carbonTableIdentifier table identifier which will give table name and database name
   * @param columnIdentifier      column unique identifier
   */
  public CarbonDictionaryMetadataReaderImpl(String hdfsStorePath,
      CarbonTableIdentifier carbonTableIdentifier, String columnIdentifier) {
    this.hdfsStorePath = hdfsStorePath;
    this.carbonTableIdentifier = carbonTableIdentifier;
    this.columnIdentifier = columnIdentifier;
    initFileLocation();
  }

  /**
   * This method will be used to read complete metadata file.
   * Applicable scenarios:
   * 1. Query execution. Whenever a query is executed then to read the dictionary file
   * and define the query scope first dictionary metadata has to be read first.
   * 2. If dictionary file is read using start and end offset then using this meta list
   * we can count the total number of dictionary chunks present between the 2 offsets
   *
   * @return list of all dictionary meta chunks which contains information for each segment
   * @throws IOException if an I/O error occurs
   */
  @Override public List<CarbonDictionaryColumnMetaChunk> read() throws IOException {
    List<CarbonDictionaryColumnMetaChunk> dictionaryMetaChunks =
        new ArrayList<CarbonDictionaryColumnMetaChunk>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    CarbonDictionaryColumnMetaChunk columnMetaChunk = null;
    ColumnDictionaryChunkMeta dictionaryChunkMeta = null;
    // open dictionary meta thrift reader
    openThriftReader();
    // read till dictionary chunk count
    while (dictionaryMetadataFileReader.hasNext()) {
      // get the thrift object for dictionary chunk
      dictionaryChunkMeta = (ColumnDictionaryChunkMeta) dictionaryMetadataFileReader.read();
      // create a new instance of chunk meta wrapper using thrift object
      columnMetaChunk = getNewInstanceOfCarbonDictionaryColumnMetaChunk(dictionaryChunkMeta);
      dictionaryMetaChunks.add(columnMetaChunk);
    }
    return dictionaryMetaChunks;
  }

  /**
   * This method will be used to read only the last entry of dictionary meta chunk.
   * Applicable scenarios :
   * 1. Global dictionary generation for incremental load. In this case only the
   * last dictionary chunk meta entry has to be read to calculate min, max surrogate
   * key and start and end offset for the new dictionary chunk.
   * 2. Truncate operation. While writing dictionary file in case of incremental load
   * dictionary file needs to be validated for any inconsistency. Here end offset of last
   * dictionary chunk meta is validated with file size.
   *
   * @return last segment entry for dictionary chunk
   * @throws IOException if an I/O error occurs
   */
  @Override public CarbonDictionaryColumnMetaChunk readLastEntryOfDictionaryMetaChunk()
      throws IOException {
    ColumnDictionaryChunkMeta dictionaryChunkMeta = null;
    // open dictionary meta thrift reader
    openThriftReader();
    // at the completion of while loop we will get the last dictionary chunk entry
    while (dictionaryMetadataFileReader.hasNext()) {
      // get the thrift object for dictionary chunk
      dictionaryChunkMeta = (ColumnDictionaryChunkMeta) dictionaryMetadataFileReader.read();
    }
    // create a new instance of chunk meta wrapper using thrift object
    CarbonDictionaryColumnMetaChunk columnMetaChunkForLastSegment =
        getNewInstanceOfCarbonDictionaryColumnMetaChunk(dictionaryChunkMeta);
    return columnMetaChunkForLastSegment;
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override public void close() throws IOException {
    if (null != dictionaryMetadataFileReader) {
      dictionaryMetadataFileReader.close();
    }
  }

  /**
   * This method will form the path for dictionary metadata file for a given column
   */
  private void initFileLocation() {
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(this.hdfsStorePath, carbonTableIdentifier);
    this.columnDictionaryMetadataFilePath =
        carbonTablePath.getDictionaryMetaFilePath(columnIdentifier);
  }

  /**
   * This method will open the dictionary file stream for reading
   *
   * @throws IOException thrift reader open method throws IOException
   */
  private void openThriftReader() throws IOException {
    // initialise dictionary file reader which will return dictionary thrift object
    // dictionary thrift object contains a list of byte buffer
    dictionaryMetadataFileReader =
        new ThriftReader(this.columnDictionaryMetadataFilePath, new ThriftReader.TBaseCreator() {
          @Override public TBase create() {
            return new ColumnDictionaryChunkMeta();
          }
        });
    // Open it
    dictionaryMetadataFileReader.open();
  }

  /**
   * Given a thrift object thie method will create a new wrapper class object
   * for dictionary chunk
   *
   * @param dictionaryChunkMeta reference for chunk meta thrift object
   * @return wrapper object of dictionary chunk meta
   */
  private CarbonDictionaryColumnMetaChunk getNewInstanceOfCarbonDictionaryColumnMetaChunk(
      ColumnDictionaryChunkMeta dictionaryChunkMeta) {
    CarbonDictionaryColumnMetaChunk columnMetaChunk =
        new CarbonDictionaryColumnMetaChunk(dictionaryChunkMeta.getMin_surrogate_key(),
            dictionaryChunkMeta.getMax_surrogate_key(), dictionaryChunkMeta.getStart_offset(),
            dictionaryChunkMeta.getEnd_offset(), dictionaryChunkMeta.getChunk_count());
    return columnMetaChunk;
  }
}
