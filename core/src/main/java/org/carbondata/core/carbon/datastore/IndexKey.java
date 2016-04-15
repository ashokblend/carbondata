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
package org.carbondata.core.carbon.datastore;

/**
 * Index class to store the index of the 
 * segment leaf node infos
 *
 */
public class IndexKey {

    /**
     * key which is generated from key generator
     */
    private byte[] dictionaryKeys;

    /**
     * dictionary which was no generated using key generator
     */
    private byte[] noDictionaryKeys;

    public IndexKey() {
    	dictionaryKeys = new byte[0];
    	noDictionaryKeys = new byte[0];
	}
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
