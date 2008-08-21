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

import java.io.IOException;
import java.util.Vector;

import org.apache.lucene.store.*;


public class IndexOptimizer {    
    
    private SegmentInfos fsSegmentInfos = null;
    private Directory directory = null;
    private int mergeFactor = IndexWriter.DEFAULT_MERGE_FACTOR;
    private boolean useCompoundFile = true;
    
    public IndexOptimizer(Directory d) throws IOException {
        this.directory = d;
        synchronized (d) {
            fsSegmentInfos = IndexObjectsFactory.getFSSegmentInfos(d);
            synchronized (fsSegmentInfos) {
                if (fsSegmentInfos.size() == 0) {
                    fsSegmentInfos.read(d);
                }
                
//                IndexWriter.printSegmentInfos("IndexOptimizer, segments at construction time: ", fsSegmentInfos);
            }
        }
    }
    
    public void setMergeFactor(int mf) {
        this.mergeFactor = mf;
    }
    
    public void setUseCompoundFile(boolean cf) {
        this.useCompoundFile = cf;
    }
    
    /** Merges all segments together into a single segment, optimizing an index
     for search. */
    public int optimize() throws IOException {
        final int size;
        synchronized (directory) {
            size = fsSegmentInfos.size();
        }
        
        if (size > 1) {
            int minSegment = size - mergeFactor;
            this.mergeSegments(minSegment < 0 ? 0 : minSegment, size);
        }
        
        return size;
    }
    
    /** Pops segments off of segmentInfos stack down to minSegment, merges them,
     and pushes the merged index onto the top of the segmentInfos stack. */
    
    private SegmentMerger merger = null;
    private Vector segmentsToDelete = null;
    private String mergedName = null;
    private int segmentsDeleteCount = -1;
    private int mergedDocCount = -1;
    private SegmentInfos tempInfos = null;
    
    /** Merges the named range of segments, replacing them in the stack with a
     * single segment. */
    private final void mergeSegments(int minSegment, int end) throws IOException {
        final String mergedName = IndexWriter.newSegmentName(fsSegmentInfos);
        SegmentMerger merger = new SegmentMerger(directory, mergedName);
        final Vector segmentsToDelete = new Vector();
        final int size = end - minSegment;
        int count = 0;
        
        for (count = 0; count < size; count++) { 
            SegmentInfo si = (SegmentInfo) fsSegmentInfos.get(count);
            IndexReader reader = SegmentReader.get(si);
            merger.add(reader);
            if (reader.directory() == this.directory) {
                segmentsToDelete.addElement(reader);   // queue segment for deletion
            }
        }
        
        mergedDocCount = merger.merge();
        
        segmentsDeleteCount = size;
        
        // close readers before we attempt to delete now-obsolete segments
        merger.closeReaders();
        
        this.mergedName = mergedName;
        this.merger = merger;
        this.segmentsToDelete = segmentsToDelete; 
    }
    
    public void optimizeSegmentInfos() throws IOException {
        int count = segmentsDeleteCount;
        synchronized (fsSegmentInfos) {
            tempInfos = fsSegmentInfos.copy();
        }
        
        while (count-- > 0) {
            tempInfos.remove(0);
        }            
        tempInfos.add(0, new SegmentInfo(mergedName, mergedDocCount, directory));
        
        synchronized (directory) {
            tempInfos.write(directory);     // commit before deleting
//            IndexWriter.printSegmentInfos("IndexOptimizer, writing to segments.new: ", tempInfos);
        } 
    }
    
    public void updateIndexUponFinalize() {
        final int size = tempInfos.size();
        int count = 0;
        synchronized (fsSegmentInfos) {
            fsSegmentInfos.clear();
            while (count++ < size) {
                fsSegmentInfos.add(tempInfos.remove(0));
            }
        }
//        IndexWriter.printSegmentInfos("IndexOptimizer, segments info after finalizing: ", fsSegmentInfos);
    }
    
    /** Does some cleaning up */
    public void cleanup() throws IOException {
        synchronized (directory) {
            if (segmentsToDelete != null) {
                IndexWriter.deleteSegments(directory, segmentsToDelete);  // delete now-unused segments
            }
            if (useCompoundFile && merger != null && mergedName != null) {
                final Vector filesToDelete = merger.createCompoundFile(mergedName + ".tmp");
                directory.renameFile(mergedName + ".tmp", mergedName + ".cfs");
                // delete now unused files of segment 
                IndexWriter.deleteFiles(directory, filesToDelete);
            }
        }

        this.mergedName = null;
        this.merger = null;
        this.segmentsToDelete.clear();
        this.segmentsToDelete = null;
        this.tempInfos.clear();
        this.tempInfos = null;
        this.mergedDocCount = -1;
        this.segmentsDeleteCount = -1;
    }
    
 }