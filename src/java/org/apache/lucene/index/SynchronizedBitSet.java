/*
 * Axiom Stack Web Application Framework
 * Copyright (C) 2008  Axiom Software Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Axiom Software Inc., 11480 Commerce Park Drive, Third Floor, Reston, VA 20191 USA
 * email: info@axiomsoftwareinc.com
 */
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