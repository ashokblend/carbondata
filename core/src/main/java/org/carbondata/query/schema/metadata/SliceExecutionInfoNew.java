package org.carbondata.query.schema.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.vo.HybridStoreModel;
import org.carbondata.query.aggregator.CustomCarbonAggregateExpression;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.datastorage.storeInterfaces.DataStoreBlock;
import org.carbondata.query.evaluators.FilterEvaluator;
import org.carbondata.query.executer.impl.RestructureHolder;

public class SliceExecutionInfoNew {



    /**
     * Key Generator
     */
    private KeyGenerator keyGenerator;

    /**
     * Start key
     */
    private long[] startKey;

    /**
     * End key
     */
    private long[] endKey;

    /**
     * Slice
     */
    private InMemoryTable slice;

    /**
     * Dimensions
     */
    private Dimension[] queryDimensions;

    /**
     *
     */
    private int maskedKeyByteSize;

    /**
     *
     */
    private String tableName;

    /**
     *
     */
    private int[] measureOrdinal;

    /**
     * Unique values represents null values of measure.
     */
    private Object[] uniqueValues;

    /**
     * schemaName
     */
    private String schemaName;

    /**
     * cubeName
     */
    private String cubeName;

    /**
     * queryId
     */
    private String queryId;

    /**
     * actualMaxKeyBasedOnDimensions
     */
    private byte[] actualMaxKeyBasedOnDimensions;

    /**
     * actalMaskedByteRanges
     */
    private int[] actalMaskedByteRanges;

    /**
     * actualMaskedKeyByteSize
     */
    private int actualMaskedKeyByteSize;

    /**
     * maskedBytePositions
     */
    private int[] maskedBytePositions;

    /**
     * actualKeyGenerator
     */
    private KeyGenerator actualKeyGenerator;

    /**
     * restructureHolder
     */
    private RestructureHolder restructureHolder;

    /**
     * dimensionSortOrder
     */
    private byte[] dimensionSortOrder;

    /**
     * dimensionMaskKey
     */
    private byte[][] dimensionMaskKeys;

    /**
     * slices
     */
    private List<InMemoryTable> slices;

    /**
     * maskedByteRangeForsorting
     */
    private int[][] maskedByteRangeForSorting;

    private Dimension[] originalDims;

    /**
     *
     */
    private int[] sortOrderAsPerActualDims;

    /**
     * isCustomMeasure
     */
    private boolean isCustomMeasure;

    /**
     * factKeyGenerator
     */
    private KeyGenerator factKeyGenerator;

    private int limit = -1;

    private boolean detailQuery;

    /**
     * columnarSplitter
     */
    private ColumnarSplitter columnarSplitter;

    /**
     * query dimension ordinal
     */
    private int[] queryDimOrdinal;

    /**
     * filterEvaluator
     */
    private FilterEvaluator filterEvaluatorTree;

    /**
     * totalNumberOfMeasuresInTable
     */
    private int totalNumberOfMeasuresInTable;

    /**
     * totalNumerOfDimColumns
     */
    private int totalNumerOfDimColumns;

    /**
     * numberOfRecordsInMemory
     */
    private int numberOfRecordsInMemory;

    /**
     * outLocation
     */
    private String outLocation;

    /**
     * dimAggInfo
     */
    private List<DimensionAggregatorInfo> dimAggInfo;

    /**
     * aggType
     */
    private String[] aggType;

    /**
     *
     */
    private List<CustomCarbonAggregateExpression> expressions;

    /**
     * minValue
     */
    private Object[] msrMinValue;

    /**
     * measurIndex
     */
    private int measureStartIndex;

    /**
     * allSelectedDimensions
     */
    private int[] allSelectedDimensions;

    /**
     * currentSliceIndex
     */
    private int currentSliceIndex;

    /**
     * partitionid
     */
    private String partitionId;

    /**
     * allSelectedMeasures
     */
    private int[] allSelectedMeasures;

    /**
     * expressionStartIndex
     */
    private int expressionStartIndex;

    /**
     * isMeasureExistis
     */
    private boolean[] isMeasureExistis;

    /**
     * msrDefaultValue
     */
    private Object[] msrDefaultValue;

    private byte[] sortedDimensionsIndex;

    private boolean isExecutionRequired = true;

    private DataStoreBlock startNode;

    private int numberOfNodeToScan;

    private boolean isFileBasedQuery;

    private boolean[] highCardinalityTypes;

    private HybridStoreModel hybridStoreMeta;
    private HashMap<Integer, Integer> measureOrdinalMap;

    private SqlStatement.Type[] dataTypes;

    private Dimension[] dimensions;
    private Map<Integer, GenericQueryType> complexQueryDimensions;

    public Dimension[] getDimensions() {
        return dimensions;
    }

    public void setDimensions(Dimension[] dimensions) {
        this.dimensions = dimensions;
    }

    public Map<Integer, GenericQueryType> getComplexQueryDimensions() {
        return complexQueryDimensions;
    }

    public void setComplexQueryDimensions(Map<Integer, GenericQueryType> complexQueryDimensions) {
        this.complexQueryDimensions = complexQueryDimensions;
    }

    public KeyGenerator getKeyGenerator() {
        return keyGenerator;
    }

    public void setKeyGenerator(final KeyGenerator keyGenerator) {
        this.keyGenerator = keyGenerator;
    }

    public long[] getStartKey() {
        return startKey;
    }

    public void setStartKey(final long[] startKey) {
        this.startKey = startKey;
    }

    public long[] getEndKey() {
        return endKey;
    }

    public void setEndKey(final long[] endKey) {
        this.endKey = endKey;
    }

    public InMemoryTable getSlice() {
        return slice;
    }

    public void setSlice(final InMemoryTable slice) {
        this.slice = slice;
    }

    public Dimension[] getQueryDimensions() {
        return queryDimensions;
    }

    public void setQueryDimensions(final Dimension[] queryDimensions) {
        this.queryDimensions = queryDimensions;
    }

    public int[] getMeasureOrdinal() {
        return measureOrdinal;
    }

    /**
     * @param measureOrdinal
     */
    public void setMeasureOrdinal(int[] measureOrdinal) {
        this.measureOrdinal = measureOrdinal;
    }

  /*  @Override
    public Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }*/

    public int getMaskedKeyByteSize() {
        return maskedKeyByteSize;
    }

    public void setMaskedKeyByteSize(final int maskedKeyByteSize) {
        this.maskedKeyByteSize = maskedKeyByteSize;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(final String factTable) {
        this.tableName = factTable;
    }

    /**
     * @return the uniqueValues
     */
    public Object[] getUniqueValues() {
        return uniqueValues;
    }

    /**
     * @param uniqueValues the uniqueValues to set
     */
    public void setUniqueValues(final Object[] uniqueValues) {
        this.uniqueValues = uniqueValues;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName() {
        return cubeName;
    }

    /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(final String cubeName) {
        this.cubeName = cubeName;
    }

    /**
     * @return the queryId
     */
    public String getQueryId() {
        return queryId;
    }

    /**
     * @param queryId the queryId to set
     */
    public void setQueryId(final String queryId) {
        this.queryId = queryId;
    }

    /**
     * @return the actualMaxKeyBasedOnDimensions
     */
    public byte[] getActualMaxKeyBasedOnDimensions() {
        return actualMaxKeyBasedOnDimensions;
    }

    /**
     * @param actualMaxKeyBasedOnDimensions the actualMaxKeyBasedOnDimensions to set
     */
    public void setActualMaxKeyBasedOnDimensions(final byte[] actualMaxKeyBasedOnDimensions) {
        this.actualMaxKeyBasedOnDimensions = actualMaxKeyBasedOnDimensions;
    }

    /**
     * @return the actalMaskedByteRanges
     */
    public int[] getActalMaskedByteRanges() {
        return actalMaskedByteRanges;
    }

    /**
     * @param actalMaskedByteRanges the actalMaskedByteRanges to set
     */
    public void setActalMaskedByteRanges(final int[] actalMaskedByteRanges) {
        this.actalMaskedByteRanges = actalMaskedByteRanges;
    }

    /**
     * @return the actualMaskedKeyByteSize
     */
    public int getActualMaskedKeyByteSize() {
        return actualMaskedKeyByteSize;
    }

    /**
     * @param actualMaskedKeyByteSize the actualMaskedKeyByteSize to set
     */
    public void setActualMaskedKeyByteSize(final int actualMaskedKeyByteSize) {
        this.actualMaskedKeyByteSize = actualMaskedKeyByteSize;
    }

    /**
     * @return the maskedBytePositions
     */
    public int[] getMaskedBytePositions() {
        return maskedBytePositions;
    }

    /**
     * @param maskedBytePositions the maskedBytePositions to set
     */
    public void setMaskedBytePositions(final int[] maskedBytePositions) {
        this.maskedBytePositions = maskedBytePositions;
    }

    /**
     * @return the actualKeyGenerator
     */
    public KeyGenerator getActualKeyGenerator() {
        return actualKeyGenerator;
    }

    /**
     * @param actualKeyGenerator the actualKeyGenerator to set
     */
    public void setActualKeyGenerator(final KeyGenerator actualKeyGenerator) {
        this.actualKeyGenerator = actualKeyGenerator;
    }

    /**
     * @return the restructureHolder
     */
    public RestructureHolder getRestructureHolder() {
        return restructureHolder;
    }

    /**
     * @param restructureHolder the restructureHolder to set
     */
    public void setRestructureHolder(final RestructureHolder restructureHolder) {
        this.restructureHolder = restructureHolder;
    }

    /**
     * @return the dimensionSortOrder
     */
    public byte[] getDimensionSortOrder() {
        return dimensionSortOrder;
    }

    /**
     * @param dimensionSortOrder the dimensionSortOrder to set
     */
    public void setDimensionSortOrder(final byte[] dimensionSortOrder) {
        this.dimensionSortOrder = dimensionSortOrder;
    }

    /**
     * @return the slices
     */
    public List<InMemoryTable> getSlices() {
        return slices;
    }

    /**
     * @param slices the slices to set
     */
    public void setSlices(final List<InMemoryTable> slices) {
        this.slices = slices;
    }

    /**
     * @return the maskedByteRangeForSorting
     */
    public int[][] getMaskedByteRangeForSorting() {
        return maskedByteRangeForSorting;
    }

    /**
     * @param maskedByteRangeForSorting the maskedByteRangeForSorting to set
     */
    public void setMaskedByteRangeForSorting(final int[][] maskedByteRangeForSorting) {
        this.maskedByteRangeForSorting = maskedByteRangeForSorting;
    }

    /**
     * @return the dimensionMaskKeys
     */
    public byte[][] getDimensionMaskKeys() {
        return dimensionMaskKeys;
    }

    /**
     * @param dimensionMaskKeys the dimensionMaskKeys to set
     */
    public void setDimensionMaskKeys(final byte[][] dimensionMaskKeys) {
        this.dimensionMaskKeys = dimensionMaskKeys;
    }

    /**
     * @return the originalDims
     */
    public Dimension[] getOriginalDims() {
        return originalDims;
    }

    /**
     * @param originalDims the originalDims to set
     */
    public void setOriginalDims(final Dimension[] originalDims) {
        this.originalDims = originalDims;
    }

    /**
     * @return the sortOrderAsPerActualDims
     */
    public int[] getSortOrderAsPerActualDims() {
        return sortOrderAsPerActualDims;
    }

    /**
     * @param sortOrderAsPerActualDims the sortOrderAsPerActualDims to set
     */
    public void setSortOrderAsPerActualDims(final int[] sortOrderAsPerActualDims) {
        this.sortOrderAsPerActualDims = sortOrderAsPerActualDims;
    }

    /**
     * @return the isCustomMeasure
     */
    public boolean isCustomMeasure() {
        return isCustomMeasure;
    }

    /**
     * @param isCustomMeasure the isCustomMeasure to set
     */
    public void setCustomMeasure(final boolean isCustomMeasure) {
        this.isCustomMeasure = isCustomMeasure;
    }

    /**
     * @return the factKeyGenerator
     */
    public KeyGenerator getFactKeyGenerator() {
        return factKeyGenerator;
    }

    /**
     * @param factKeyGenerator the factKeyGenerator to set
     */
    public void setFactKeyGenerator(final KeyGenerator factKeyGenerator) {
        this.factKeyGenerator = factKeyGenerator;
    }

    /**
     * @return the limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * @param limit the limit to set
     */
    public void setLimit(final int limit) {
        this.limit = limit;
    }

    /**
     * @return the detailQuery
     */
    public boolean isDetailQuery() {
        return detailQuery;
    }

    /**
     * @param detailQuery the detailQuery to set
     */
    public void setDetailQuery(final boolean detailQuery) {
        this.detailQuery = detailQuery;
    }

    /**
     * @return the columnarSplitter
     */
    public ColumnarSplitter getColumnarSplitter() {
        return columnarSplitter;
    }

    /**
     * @param columnarSplitter the columnarSplitter to set
     */
    public void setColumnarSplitter(final ColumnarSplitter columnarSplitter) {
        this.columnarSplitter = columnarSplitter;
    }

    public int[] getQueryDimOrdinal() {
        return queryDimOrdinal;
    }

    public void setQueryDimOrdinal(final int[] queryDimOrdinal) {
        this.queryDimOrdinal = queryDimOrdinal;
    }

    public FilterEvaluator getFilterEvaluatorTree() {
        return filterEvaluatorTree;
    }

    public void setFilterEvaluatorTree(final FilterEvaluator filterEvaluatorTree) {
        this.filterEvaluatorTree = filterEvaluatorTree;
    }

    public int getTotalNumberOfMeasuresInTable() {
        return totalNumberOfMeasuresInTable;
    }

    public void setTotalNumberOfMeasuresInTable(final int totalNumberOfMeasuresInTable) {
        this.totalNumberOfMeasuresInTable = totalNumberOfMeasuresInTable;
    }

    public int getTotalNumerOfDimColumns() {
        return totalNumerOfDimColumns;
    }

    public void setTotalNumerOfDimColumns(final int totalNumerOfDimColumns) {
        this.totalNumerOfDimColumns = totalNumerOfDimColumns;
    }

    public int getNumberOfRecordsInMemory() {
        return numberOfRecordsInMemory;
    }

    public void setNumberOfRecordsInMemory(final int numberOfRecordsInMemory) {
        this.numberOfRecordsInMemory = numberOfRecordsInMemory;
    }

    public String getOutLocation() {
        return outLocation;
    }

    public void setOutLocation(final String outLocation) {
        this.outLocation = outLocation;
    }

    public List<DimensionAggregatorInfo> getDimAggInfo() {
        return dimAggInfo;
    }

    public void setDimAggInfo(List<DimensionAggregatorInfo> dimAggInfo) {
        this.dimAggInfo = dimAggInfo;
    }

    public String[] getAggType() {
        return aggType;
    }

    public void setAggType(String[] aggType) {
        this.aggType = aggType;
    }

    public List<CustomCarbonAggregateExpression> getCustomExpressions() {
        return expressions;
    }

    public void setCustomExpressions(List<CustomCarbonAggregateExpression> expressions) {
        this.expressions = expressions;
    }

    public Object[] getMsrMinValue() {
        return msrMinValue;
    }

    public void setMsrMinValue(Object[] msrMinValue) {
        this.msrMinValue = msrMinValue;
    }

    public int[] getAllSelectedDimensions() {
        return allSelectedDimensions;
    }

    public void setAllSelectedDimensions(int[] allSelectedDimensions) {
        this.allSelectedDimensions = allSelectedDimensions;
    }

    public int getCurrentSliceIndex() {
        return currentSliceIndex;
    }

    public void setCurrentSliceIndex(int currentSliceIndex) {
        this.currentSliceIndex = currentSliceIndex;
    }

    public int getMeasureStartIndex() {
        return measureStartIndex;
    }

    public void setMeasureStartIndex(int measureStartIndex) {
        this.measureStartIndex = measureStartIndex;
    }

    public String getPartitionId() {
        return this.partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;

    }

    public int[] getAllSelectedMeasures() {
        return allSelectedMeasures;
    }

    public void setAllSelectedMeasures(int[] allSelectedMeasures) {
        this.allSelectedMeasures = allSelectedMeasures;
    }

    public int getExpressionStartIndex() {
        return expressionStartIndex;
    }

    public void setExpressionStartIndex(int expressionStartIndex) {
        this.expressionStartIndex = expressionStartIndex;
    }

    public Object[] getMsrDefaultValue() {
        return msrDefaultValue;
    }

    public void setMsrDefaultValue(Object[] msrDefaultValue) {
        this.msrDefaultValue = msrDefaultValue;
    }

    public boolean[] getIsMeasureExistis() {
        return isMeasureExistis;
    }

    public void setIsMeasureExistis(boolean[] isMeasureExistis) {
        this.isMeasureExistis = isMeasureExistis;
    }

    public byte[] getSortedDimensionsIndex() {
        return sortedDimensionsIndex;
    }

    public void setSortedDimensionsIndex(byte[] fillSortedDimensions) {
        this.sortedDimensionsIndex = fillSortedDimensions;
    }

    /**
     * @return Returns the isExecutionRequired.
     */
    public boolean isExecutionRequired() {
        return isExecutionRequired;
    }

    /**
     * @param isExecutionRequired The isExecutionRequired to set.
     */
    public void setExecutionRequired(boolean isExecutionRequired) {
        this.isExecutionRequired = isExecutionRequired;
    }

    public DataStoreBlock getStartNode() {
        return startNode;
    }

    public void setStartNode(DataStoreBlock startNode) {
        this.startNode = startNode;
    }

    public int getNumberOfNodeToScan() {
        return numberOfNodeToScan;
    }

    public void setNumberOfNodeToScan(int numberOfNodeToScan) {
        this.numberOfNodeToScan = numberOfNodeToScan;
    }

    public boolean isFileBasedQuery() {
        return isFileBasedQuery;
    }

    public void setFileBasedQuery(boolean isFileBasedQuery) {
        this.isFileBasedQuery = isFileBasedQuery;
    }

    public HybridStoreModel getHybridStoreMeta() {
        return this.hybridStoreMeta;
    }

    public void setHybridStoreMeta(HybridStoreModel hybridStoreMeta) {
        this.hybridStoreMeta = hybridStoreMeta;

    }

    public SqlStatement.Type[] getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(SqlStatement.Type[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    /**
     * setHighCardinalityType.
     *
     * @param highCardinalityTypes
     */
    public void setHighCardinalityType(boolean[] highCardinalityTypes) {
        this.highCardinalityTypes = highCardinalityTypes;

    }

    /**
     * getHighCardinalityTypes.
     *
     * @return
     */
    public boolean[] getHighCardinalityTypes() {
        return highCardinalityTypes;
    }

    public HashMap<Integer, Integer> getMeasureOrdinalMap() {
        return measureOrdinalMap;
    }

    public void setMeasureOrdinalMap(HashMap<Integer, Integer> measureOrdinal) {
        this.measureOrdinalMap = measureOrdinal;
    }

}
