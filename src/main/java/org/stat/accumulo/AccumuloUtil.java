package org.stat.accumulo;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.client.lexicoder.impl.ByteUtils;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;

/**
 * Accumulo utilities.
 */
public class AccumuloUtil {
    public static final IntegerLexicoder INT_LEXICODER = new IntegerLexicoder();
    public static final DateLexicoder DATE_LEXICODER = new DateLexicoder();
    public static final StringLexicoder STRING_LEXICODER = new StringLexicoder();
    public static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Split the byte array based on the Accumulo encoding.
     * @param value The byte array value that contains the set of escaped values.
     * @return Split the value.
     */
    public static byte[][] splitValues(Value value) {
        return ByteUtils.split(value.get());
    }

    /**
     * Convert a byte array that was encoded and escaped back to an int.
     * @param encodedEscapedInt The byte array that was encoded and escaped.
     * @return The int value
     */
    public static int bytesToInt(byte[] encodedEscapedInt) {
        return INT_LEXICODER.decode(ByteUtils.unescape(encodedEscapedInt));
    }

    public static byte[] intToEscapedBytes(int intValue) {
        return escapeBytes(intToBytes(intValue));
    }

    public static byte[] intToBytes(int intValue) {
        return INT_LEXICODER.encode(intValue);
    }

    /**
     * Escaping the bytes is the processing converting a 0 to some other value so that when concatenating
     * values together with 0 as the separator, the 0 in the actual value does not get misunderstood
     * @param bytes The value to escape.
     * @return The escaped value
     */
    public static byte[] escapeBytes(byte[] bytes) {
        return ByteUtils.escape(bytes);
    }

    public static byte[] concatInts(int[] ints) {
        byte[][] bytes = new byte[ints.length][];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = intToEscapedBytes(ints[i]);
        }

        return ByteUtils.concat(bytes);
    }

    public static Instance getInstance() {
        ClientConfiguration clientConfiguration = ClientConfiguration.loadDefault();
        return new ZooKeeperInstance(clientConfiguration);
    }

    public static Collection<Range> getDateRange(Date start, Date end) {
        // Empty start and end means everything
        if (start == null && end == null) {
            return Collections.singleton(new Range());
        }

        Text encodedStart = null;
        if (start != null) encodedStart = new Text(DATE_LEXICODER.encode(start));
        Text encodedEnd = null;
        if (end != null) encodedEnd = new Text(DATE_LEXICODER.encode(end));

        return Collections.singleton(new Range(encodedStart, encodedEnd));
    }
}
