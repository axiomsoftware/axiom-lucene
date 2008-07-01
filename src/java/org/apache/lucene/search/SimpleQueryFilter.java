package org.apache.lucene.search;

import java.io.IOException;
import java.util.BitSet;

import org.apache.lucene.index.IndexReader;

public class SimpleQueryFilter extends Filter {
    
    private Query query;
    private BitSet bits;

    public SimpleQueryFilter(Query query) {
        this.query = query;
    }
    
    public SimpleQueryFilter(BitSet bits1, BitSet bits2) {
        this.bits = (BitSet) bits1.clone();
        this.bits.and(bits2);
    }

    public BitSet bits(IndexReader reader) throws IOException {

        if (this.bits != null) {
            return this.bits;
        }

        final BitSet bits = new BitSet(reader.maxDoc());

        new IndexSearcher(reader).search(this.query, new HitCollector() {
            public final void collect(int doc, float score) {
                bits.set(doc);  // set bit for hit
            }
        });

        this.bits = bits;

        return bits;
    }

    public String toString() {
        return "SimpleQueryFilter(" + query + ")";
    }

    public boolean equals(Object o) {
        if (!(o instanceof SimpleQueryFilter)) return false;
        return this.query.equals(((SimpleQueryFilter)o).query);
    }

    public int hashCode() {
        return this.query.hashCode() ^ 0x923F64B9;  
    }
    
}