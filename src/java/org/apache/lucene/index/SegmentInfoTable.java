package org.apache.lucene.index;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.lucene.store.*;


public class SegmentInfoTable extends Hashtable {
    
    /** The file format version, a negative number. */
    /* Works since counter, the old 1st entry, is always >= 0 */
    public static final int FORMAT = -1;
    
    public int counter = 0;    // used to name new segments
    /**
     * counts how often the index has been changed by adding or deleting docs.
     * starting with the current time in milliseconds forces to create unique version numbers.
     */
    private long version = System.currentTimeMillis();
    
    public final SegmentInfo info(String key) {
        return (SegmentInfo) get(key);
    }
    
    public final void read(Directory directory) throws IOException {
        
        IndexInput input = directory.openInput(IndexFileNames.SEGMENTS);
        try {
            int format = input.readInt();
            if(format < 0){     // file contains explicit format info
                // check that it is a format we can understand
                if (format < FORMAT)
                    throw new IOException("Unknown format version: " + format);
                version = input.readLong(); // read version
                counter = input.readInt(); // read counter
            }
            else{     // file is in old format without explicit format info
                counter = format;
            }
            
            for (int i = input.readInt(); i > 0; i--) { // read segmentInfos
                SegmentInfo si =
                    new SegmentInfo(input.readString(), input.readInt(), directory);
                put(si.name, si);
            }
            
            if(format >= 0){    // in old format the version number may be at the end of the file
                if (input.getFilePointer() >= input.length())
                    version = System.currentTimeMillis(); // old file format without version number
                else
                    version = input.readLong(); // read version
            }
        }
        finally {
            input.close();
        }
    }
    
    public final void write(Directory directory) throws IOException {
        IndexOutput output = directory.createOutput("segments.new");
        try {
            output.writeInt(FORMAT); // write FORMAT
            output.writeLong(++version); // every write changes the index
            output.writeInt(counter); // write counter
            output.writeInt(size()); // write infos
            Iterator iter = entrySet().iterator();
            while (iter.hasNext()) {
                SegmentInfo si = (SegmentInfo) iter.next();
                output.writeString(si.name);
                output.writeInt(si.docCount);
            }         
        }
        finally {
            output.close();
        }
        
        // install new segment info
        directory.renameFile("segments.new", IndexFileNames.SEGMENTS);
    }
    
    /**
     * version number when this SegmentInfos was generated.
     */
    public long getVersion() {
        return version;
    }
    
    /**
     * Current version number from segments file.
     */
    public static long readCurrentVersion(Directory directory)
    throws IOException {
        
        IndexInput input = directory.openInput(IndexFileNames.SEGMENTS);
        int format = 0;
        long version = 0;
        try {
            format = input.readInt();
            if(format < 0){
                if (format < FORMAT)
                    throw new IOException("Unknown format version: " + format);
                version = input.readLong(); // read version
            }
        }
        finally {
            input.close();
        }
        
        if(format < 0)
            return version;
        
        // We cannot be sure about the format of the file.
        // Therefore we have to read the whole file and cannot simply seek to the version entry.
        
        SegmentInfos sis = new SegmentInfos();
        sis.read(directory);
        return sis.getVersion();
    }
    
}