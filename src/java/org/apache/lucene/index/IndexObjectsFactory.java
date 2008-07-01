package org.apache.lucene.index;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.store.*;


public class IndexObjectsFactory {
    
    final private static Hashtable FSSEGMENTS = new Hashtable();
    
    final private static Hashtable BITSETS = new Hashtable();
    
    final private static HashMap LOCKS = new HashMap();
    
    
    public static SegmentInfos getFSSegmentInfos(Directory dir) {
        SegmentInfos segmentInfos = (SegmentInfos) FSSEGMENTS.get(dir);
        
        if (segmentInfos == null) {
            segmentInfos = new SegmentInfos();
            FSSEGMENTS.put(dir, segmentInfos);
        }
        
        return segmentInfos;
    }
    
    public static void setFSSegmentInfos(Directory dir, SegmentInfos segmentInfos) {
        FSSEGMENTS.put(dir, segmentInfos);
    }
    
    public static DeletedInfos getDeletedInfos(Directory dir) throws IOException {
        return (DeletedInfos) BITSETS.get(dir);
    }
    
    public static void initDeletedInfos(Directory dir) throws IOException {
        if (BITSETS.get(dir) == null) {
            DeletedInfos infos = DeletedInfos.initializeDeletedInfosFromIndex(dir);
            BITSETS.put(dir, infos);
        }
    }
    
    public static void setDeletedInfos(Directory dir, DeletedInfos infos) {
        BITSETS.put(dir, infos);
    }
    
    public synchronized static Object getDirectoryLock(Directory dir) {
        Object lock = LOCKS.get(dir);
        if (lock == null) {
            lock = new Object();
            LOCKS.put(dir, lock);
        }
        return lock;
    }
    
    public static void removeDeletedInfos(Directory dir) {
        BITSETS.remove(dir);
    }
    
}