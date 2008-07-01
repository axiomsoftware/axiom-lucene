package org.apache.lucene.index;

import java.util.*;
import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;


public class SynchronizedBitSet extends BitSet { 
    
    public SynchronizedBitSet() {
        super();
    }
    
    public SynchronizedBitSet(int initnum) {
        super(initnum);
    }
    
    public synchronized boolean get(int n) { 
        return super.get(n); 
    }
    
    public synchronized void set(int n) { 
        super.set(n); 
    }
    
    public SynchronizedBitSet copy() {
        int size = this.size();
        SynchronizedBitSet sbs = new SynchronizedBitSet(size);
        for (int i = 0; i < size; i++) {
            if (this.get(i)) {
                sbs.set(i);
            }
        }
        return sbs;
    }
    
    public Object clone() {
        return copy();
    }
    
}