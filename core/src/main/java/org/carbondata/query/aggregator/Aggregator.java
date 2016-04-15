package org.carbondata.query.aggregator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;

public interface Aggregator {

	/**
     * Below method will be used to aggregate the Double value
     *
     * @param newVal
     */
    void agg(double newVal);
    
    /**
     * Below method will be used to aggregate the Double value
     *
     * @param newVal
     */
    void agg(long newVal);

    /**
     * Below method will be used to aggregate the object value
     *
     * @param newVal
     */
    void agg(Object newVal);

    /**
     * Below method will be used to aggregate the value based on index
     *
     * @param newVal
     * @param index
     */
    void agg(MeasureColumnDataChunk newVal, int index);

    /**
     * Get the Serialize byte array
     *
     * @return
     */
    byte[] getByteArray();
    
    /**
     * This method will be used to set the new value
     *
     * @param newValue
     */
    void setNewValue(Object newValue);
    
    /**
     * This method return the object value of the MeasureAggregator
     *
     * @return aggregated value
     */
    Object getValueObject();

    /**
     * This method return the object value of the MeasureAggregator
     *
     * @return aggregated value
     */
    Double getDoubleValue();

    /**
     * This method return the object value of the MeasureAggregator
     *
     * @return aggregated value
     */
    Long getLongValue();

    BigDecimal getBigDecimalValue();

    /**
     * This method merge the aggregated value based on aggregator passed
     *
     * @param aggregator type of aggregator
     */
    void merge(Aggregator aggregator);

    /**
     * Is first time. It means it was never used for aggregating any value.
     *
     * @return
     */
    boolean isFirstTime();

    /**
     * it creates the new copy of MeasureAggregator
     *
     * @return MeasureAggregator
     */
    Aggregator getCopy();

    /**
     * Write the state of the class to buffer
     */
    void writeData(DataOutput output) throws IOException;

    /**
     * Read the state of the class and set to the object
     */
    void readData(DataInput inPut) throws IOException;

    Aggregator get();
    
    /**
     * Below method will be used to get the 
     * new instance
     * @return new instance
     */
    Aggregator getNew();

}
