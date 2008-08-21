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
import java.io.IOException;
import java.io.Serializable;

import org.apache.lucene.document.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.*;


public class DeletedInfos extends Hashtable implements Serializable {
    
    public final static String ID = "_id";
    public final static String LAYER = "_mode";
    public final static String DELETED = "_deleted";
    public final static String LASTMODIFIED = "_lastmodified";
    public final static String KEY_SEPERATOR = "_L";
    
    private SynchronizedBitSet bitset;
    
    private int docCount = 0;
    
    public DeletedInfos() {
        super();
    }
    
    DeletedInfos(Hashtable table) {
        super(table);
    }
    
    DeletedInfos(HashMap table) {
        super(table);
    }
    
    public SynchronizedBitSet getBitSet() {
        return bitset;
    }
    
    public void setBitSet(SynchronizedBitSet sbs) {
        this.bitset = sbs;
    }
    
    public void setDocDeletion(int docid) {
        bitset.set(docid);
    }
    
    public boolean isDocDeleted(int docid) {
        return bitset.get(docid);
    }
    
    public synchronized int getDocCount() {
        return docCount;
    }
    
    public synchronized void setDocCount(int docCount) {
        this.docCount = docCount;
    }
    
    public synchronized void incrementDocCount() {
        docCount++;
    }
    
    public static DeletedInfos initializeDeletedInfosFromIndex(Directory directory) 
    throws IOException {
        
        DeletedInfos infos = new DeletedInfos();
        
        SynchronizedBitSet bitset = null;
        IndexSearcher searcher = null;
        
        try {
            searcher = new IndexSearcher(directory);
        } catch (Exception ex) {
            bitset = new SynchronizedBitSet();
            infos.bitset = new SynchronizedBitSet();
            infos.docCount = 0;
            if (searcher != null) {
                searcher.close();
                searcher = null;
            }
            return infos;
        }
        
        try {
            
            final int totalDocs = searcher.getIndexReader().numDocs();
            bitset = new SynchronizedBitSet(totalDocs);
            
            String delmarker = IndexWriter.DELETE_MARKER;
            HashMap map = new HashMap();
            Integer delval = new Integer(-1);
            final String ID = DeletedInfos.ID;
            final String LAYER = DeletedInfos.LAYER;
            final String DELETED = DeletedInfos.DELETED;
            final String LASTMODIFIED = DeletedInfos.LASTMODIFIED;
            
            for (int i = 0; i < totalDocs; i++) {
                Document d = searcher.doc(i);
                Field idField = d.getField(ID);
                Field layerField = d.getField(LAYER);
                
                // ignore all the "special" documents with no ids (e.g. the prototype mapping docs)
                if (idField == null || layerField == null) { 
                    continue;
                }
                
                String key = idField.stringValue() + KEY_SEPERATOR + layerField.stringValue(); 
                ArrayList listOfDocIds = (ArrayList) map.get(key);
                Integer intval = new Integer(i);
                if (listOfDocIds == null) {
                    listOfDocIds = new ArrayList();
                    map.put(key, listOfDocIds);
                    infos.put(key, intval);
                }
                listOfDocIds.add(intval);
                
                if (!delval.equals(infos.get(key)) || !"0".equals(layerField)) {
                    Field df = d.getField(DELETED);
                    if (df != null && delmarker.equals(df.stringValue())) {
                        infos.put(key, delval);
                        continue;
                    } 
                    int ival = ((Integer)infos.get(key)).intValue();
                    Field cf1 = null;
                    if (ival > delval.intValue()) {
                    	searcher.doc(ival).getField("lastmodified");
                    }
                    Field cf2 = d.getField(LASTMODIFIED);
                    if (cf1 == null && cf2 != null) {
                        infos.put(key, intval);
                    } else if (cf1 != null && cf2 != null && Long.parseLong(cf2.stringValue()) > Long.parseLong(cf1.stringValue())) {
                        infos.put(key, intval);
                    }
                }
            }
            
            Iterator iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String key = iter.next().toString();
                Integer intval = (Integer) infos.get(key);
                ArrayList listOfDocIds = (ArrayList) map.get(key);
                
                int dval = intval.intValue();
                int size = listOfDocIds.size();
                
                if (dval == -1) {
                    infos.remove(key);
                    for (int i = 0; i < size; i++) {
                        bitset.set(((Integer) listOfDocIds.get(i)).intValue());
                    }
                } else {
                    for (int i = 0; i < size; i++) {
                        int curr = ((Integer) listOfDocIds.get(i)).intValue();
                        if (curr != dval) {
                            bitset.set(curr);
                        } 
                    }
                }
            }
            
            infos.bitset = bitset;
            infos.docCount = totalDocs;
            
        } catch (IOException ioe) {
            throw new IOException("Error initializing the BitSet of deleted documents from the index.\n" + ioe.getMessage());
        } finally {
            if (searcher != null) {
                searcher.close();
                searcher = null;
            }
        }
        
        return infos;
    }
    
    public SynchronizedBitSet copyBitSet() {
        if (this.bitset == null) 
            return null;
        return this.bitset.copy();
    }
    
}