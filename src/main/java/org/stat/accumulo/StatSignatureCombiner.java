package org.stat.accumulo;

import org.apache.accumulo.core.client.lexicoder.impl.ByteUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import java.util.Iterator;

import static java.lang.Long.parseLong;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * A combiner which acts as a reducer of minhash signature. This could be done on the fly in a Spark job, but
 * this will be faster for querying purposes (but ultimately, slowing down ingest speed). It is assumed that
 * querying speed takes precedence over ingest speed and storage.
 */
public class StatSignatureCombiner extends Combiner {
    // Declare these here so they do not get recreated every time during an iteration
    private int v;
    private Value val;

    @Override
    public Value reduce(Key key, Iterator<Value> iter) {

        // The key is:
        // 1. rowid = the crawl date
        // 2. cq = device
        //
        // The value is the Accumulo concatenation of the current set of k hash values for an element of a
        // set of device values on the given crawl date
        //
        // To get the minhash signature, find the lowest value of each hash value, then concatenate the lowest
        // values back into the single value

        int[] minhashValues = null;
        while (iter.hasNext()) {
            val = iter.next();

            // Never process an empty value
            if (val.get().length == 0) {
                continue;
            }

            byte[][] splitValues = AccumuloUtil.splitValues(val);

            // Lazily initialize the minhash values
            if (minhashValues == null) {
                minhashValues = new int[splitValues.length];
                for (int i = 0; i < splitValues.length; i++) {
                    minhashValues[i] = AccumuloUtil.bytesToInt(splitValues[i]);
                }
            } else {
                for (int i = 0; i < splitValues.length; i++) {
                    // Don't know why a cast is needed since the method signature says it returns an Integer
                    v = AccumuloUtil.bytesToInt(splitValues[i]);
                    if (v < minhashValues[i]) minhashValues[i] = v;
                }
            }
        }

        // Recreate the minhash value, first by encoding, then escaping
        if (minhashValues != null) {
            byte[][] encodedEscapedMinhashValues = new byte[minhashValues.length][];
            for (int i = 0; i < minhashValues.length; i++) {
                encodedEscapedMinhashValues[i] = AccumuloUtil.intToEscapedBytes(minhashValues[i]);
            }

            return new Value(ByteUtils.concat(encodedEscapedMinhashValues));
        }

        // Getting here means that there were no values which should never happen, so return an empty value
        return new Value();
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.setName("StatSignatureCombiner");
        io.setDescription("Combiner that keeps track of the minhash signature value for a particular key");
        return io;
    } }
