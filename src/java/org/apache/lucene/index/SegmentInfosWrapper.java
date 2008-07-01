package org.apache.lucene.index;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

public class SegmentInfosWrapper {
	private SegmentInfos sis = null;
	
	public SegmentInfosWrapper(SegmentInfos sis){
		this.sis = sis;
	}
	
	public int size(){
		return this.sis.size();
	}
	
	public String getSegmentInfos(int i){
		SegmentInfo si = (SegmentInfo) sis.get(i);
		return si.toString();
	}
    
    public void restoreSegments(String[] segments, Directory dir) {
        final int size = sis.size();
        for (int i = 0; i < size; i++) {
            sis.remove(0);
        }
        
        IndexReader ir = null;
        int numDocs = 0;
        try {
            ir = new IndexSearcher(dir).getIndexReader();
            numDocs = ir.numDocs();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try { ir.close(); } catch (Exception ioex) { }
        }
        
        
        for (int i = 0; i < segments.length; i++) {
            System.out.println("Setting numdocs to " + numDocs);
            sis.add(new SegmentInfo(segments[i], numDocs, dir));
        }
        
        try {
            sis.write(dir);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}