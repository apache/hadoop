package org.apache.hadoop.hdfs;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestDFSInputStream {
    @Test
    public void testOpTypeSymbolsAreUnique() {
        final Set<String> opTypeSymbols = new HashSet<>();
        for (DFSOpsCountStatistics.OpType opType : DFSOpsCountStatistics.OpType.values()) {
            assertFalse(opTypeSymbols.contains(opType.getSymbol()));
            opTypeSymbols.add(opType.getSymbol());
        }
        assertEquals(DFSOpsCountStatistics.OpType.values().length, opTypeSymbols.size());
    }
}
