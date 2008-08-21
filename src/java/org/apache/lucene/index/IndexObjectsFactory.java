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