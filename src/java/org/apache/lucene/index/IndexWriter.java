package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* 
 * Modified by:
 * 
 * Axiom Software Inc., 11480 Commerce Park Drive, Third Floor, Reston, VA 20191 USA
 * email: info@axiomsoftwareinc.com
 */

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;


/**
 An IndexWriter creates and maintains an index.
 
 The third argument to the 
 <a href="#IndexWriter(org.apache.lucene.store.Directory, org.apache.lucene.analysis.Analyzer, boolean)"><b>constructor</b></a>
 determines whether a new index is created, or whether an existing index is
 opened for the addition of new documents.
 
 In either case, documents are added with the <a
 href="#addDocument(org.apache.lucene.document.Document)"><b>addDocument</b></a> method.  
 When finished adding documents, <a href="#close()"><b>close</b></a> should be called.
 
 <p>If an index will not have more documents added for a while and optimal search
 performance is desired, then the <a href="#optimize()"><b>optimize</b></a>
 method should be called before the index is closed.
 
 <p>Opening an IndexWriter creates a lock file for the directory in use. Trying to open
 another IndexWriter on the same directory will lead to an IOException. The IOException
 is also thrown if an IndexReader on the same directory is used to delete documents
 from the index.
 
 @see IndexModifier IndexModifier supports the important methods of IndexWriter plus deletion
 */

public class IndexWriter {
    /**
     * Default value is 1,000.
     */
    public final static long WRITE_LOCK_TIMEOUT = 1000;
    
    /**
     * Default value is 10,000.
     */
    public final static long COMMIT_LOCK_TIMEOUT = 10000;
    
    public static final String WRITE_LOCK_NAME = "write.lock";
    public static final String COMMIT_LOCK_NAME = "commit.lock";
    
    /**
     * Default value is 10. Change using {@link #setMergeFactor(int)}.
     */
    public final static int DEFAULT_MERGE_FACTOR = 10;
    
    /**
     * Default value is 10. Change using {@link #setMaxBufferedDocs(int)}.
     */
    public final static int DEFAULT_MAX_BUFFERED_DOCS = 10;
    
    /**
     * @deprecated use {@link #DEFAULT_MAX_BUFFERED_DOCS} instead
     */
    public final static int DEFAULT_MIN_MERGE_DOCS = DEFAULT_MAX_BUFFERED_DOCS;
    
    /**
     * Default value is {@link Integer#MAX_VALUE}. Change using {@link #setMaxMergeDocs(int)}.
     */
    public final static int DEFAULT_MAX_MERGE_DOCS = Integer.MAX_VALUE;
    
    /**
     * Default value is 10,000. Change using {@link #setMaxFieldLength(int)}.
     */
    public final static int DEFAULT_MAX_FIELD_LENGTH = 10000;
    
    /**
     * Default value is 128. Change using {@link #setTermIndexInterval(int)}.
     */
    public final static int DEFAULT_TERM_INDEX_INTERVAL = 128;
    
    protected final static String DELETE_MARKER = "true";
    
    private Directory directory;  // where this index resides
    
    private Analyzer analyzer;    // how to analyze text
    
    private Similarity similarity = Similarity.getDefault(); // how to normalize
    
    private SegmentInfos fsSegmentInfos = null; // the segments
    
    private SegmentInfos ramSegmentInfos = new SegmentInfos(); 
    
    private SegmentInfos tempSegmentInfos = new SegmentInfos();
    
    private ArrayList obsoleteDocuments = new ArrayList();
    
    private ArrayList deletedDocuments = new ArrayList();
    
    private HashMap updatedDocuments = new HashMap();
    
    private int addedDocumentCount = -1;
    
    private Vector segmentsToDelete = new Vector();
    
    private HashMap segmentMergers = new HashMap();
    
    private final Directory ramDirectory = new RAMDirectory(); // for temp segs
    
    private int termIndexInterval = DEFAULT_TERM_INDEX_INTERVAL;
    
    /** Use compound file setting. Defaults to true, minimizing the number of
     * files used.  Setting this to false may improve indexing performance, but
     * may also cause file handle problems.
     */
    private boolean useCompoundFile = true;  
    
    private boolean closeDir;
    
    /** Get the current setting of whether to use the compound file format.
     *  Note that this just returns the value you set with setUseCompoundFile(boolean)
     *  or the default. You cannot use this to query the status of an existing index.
     *  @see #setUseCompoundFile(boolean)
     */
    public boolean getUseCompoundFile() {
        return useCompoundFile;
    }
    
    /** Setting to turn on usage of a compound file. When on, multiple files
     *  for each segment are merged into a single file once the segment creation
     *  is finished. This is done regardless of what directory is in use.
     */
    public void setUseCompoundFile(boolean value) {
        useCompoundFile = value;
    }
    
    /** Expert: Set the Similarity implementation used by this IndexWriter.
     *
     * @see Similarity#setDefault(Similarity)
     */
    public void setSimilarity(Similarity similarity) {
        this.similarity = similarity;
    }
    
    /** Expert: Return the Similarity implementation used by this IndexWriter.
     *
     * <p>This defaults to the current value of {@link Similarity#getDefault()}.
     */
    public Similarity getSimilarity() {
        return this.similarity;
    }
    
    /** Expert: Set the interval between indexed terms.  Large values cause less
     * memory to be used by IndexReader, but slow random-access to terms.  Small
     * values cause more memory to be used by an IndexReader, and speed
     * random-access to terms.
     *
     * This parameter determines the amount of computation required per query
     * term, regardless of the number of documents that contain that term.  In
     * particular, it is the maximum number of other terms that must be
     * scanned before a term is located and its frequency and position information
     * may be processed.  In a large index with user-entered query terms, query
     * processing time is likely to be dominated not by term lookup but rather
     * by the processing of frequency and positional data.  In a small index
     * or when many uncommon query terms are generated (e.g., by wildcard
     * queries) term lookup may become a dominant cost.
     *
     * In particular, <code>numUniqueTerms/interval</code> terms are read into
     * memory by an IndexReader, and, on average, <code>interval/2</code> terms
     * must be scanned for each random term access.
     *
     * @see #DEFAULT_TERM_INDEX_INTERVAL
     */
    public void setTermIndexInterval(int interval) {
        this.termIndexInterval = interval;
    }
    
    /** Expert: Return the interval between indexed terms.
     *
     * @see #setTermIndexInterval(int)
     */
    public int getTermIndexInterval() { return termIndexInterval; }
    
    /**
     * Constructs an IndexWriter for the index in <code>path</code>.
     * Text will be analyzed with <code>a</code>.  If <code>create</code>
     * is true, then a new, empty index will be created in
     * <code>path</code>, replacing the index already there, if any.
     *
     * @param path the path to the index directory
     * @param a the analyzer to use
     * @param create <code>true</code> to create the index or overwrite
     *  the existing one; <code>false</code> to append to the existing
     *  index
     * @throws IOException if the directory cannot be read/written to, or
     *  if it does not exist, and <code>create</code> is
     *  <code>false</code>
     */
    public IndexWriter(String path, Analyzer a, boolean create) throws IOException {
        this(FSDirectory.getDirectory(path, create), a, create, true);
    }
    
    /**
     * Constructs an IndexWriter for the index in <code>path</code>.
     * Text will be analyzed with <code>a</code>.  If <code>create</code>
     * is true, then a new, empty index will be created in
     * <code>path</code>, replacing the index already there, if any.
     *
     * @param path the path to the index directory
     * @param a the analyzer to use
     * @param create <code>true</code> to create the index or overwrite
     *  the existing one; <code>false</code> to append to the existing
     *  index
     * @throws IOException if the directory cannot be read/written to, or
     *  if it does not exist, and <code>create</code> is
     *  <code>false</code>
     */
    public IndexWriter(File path, Analyzer a, boolean create) throws IOException {
        this(FSDirectory.getDirectory(path, create), a, create, true);
    }
    
    /**
     * Constructs an IndexWriter for the index in <code>d</code>.
     * Text will be analyzed with <code>a</code>.  If <code>create</code>
     * is true, then a new, empty index will be created in
     * <code>d</code>, replacing the index already there, if any.
     *
     * @param d the index directory
     * @param a the analyzer to use
     * @param create <code>true</code> to create the index or overwrite
     *  the existing one; <code>false</code> to append to the existing
     *  index
     * @throws IOException if the directory cannot be read/written to, or
     *  if it does not exist, and <code>create</code> is
     *  <code>false</code>
     */
    public IndexWriter(Directory d, Analyzer a, boolean create) throws IOException {
        this(d, a, create, false);
    }
    
    private IndexWriter(Directory d, Analyzer a, final boolean create, boolean closeDir) throws IOException {
        this.closeDir = closeDir;
        this.directory = d;
        this.analyzer = a;
        
        synchronized (directory) {
            
            fsSegmentInfos = IndexObjectsFactory.getFSSegmentInfos(directory); 
            
            synchronized (fsSegmentInfos) {
                if (!d.fileExists(IndexFileNames.SEGMENTS)) {
                    fsSegmentInfos.clear();
                    fsSegmentInfos.write(directory);
                } else {
                    if (fsSegmentInfos.size() == 0) {
                        fsSegmentInfos.read(directory);
                    }
                }
//                IndexWriter.printSegmentInfos("IndexWriter, segments at construction time: ", fsSegmentInfos);
            }
        }
    }
    
    /** Determines the largest number of documents ever merged by addDocument().
     * Small values (e.g., less than 10,000) are best for interactive indexing,
     * as this limits the length of pauses while indexing to a few seconds.
     * Larger values are best for batched indexing and speedier searches.
     *
     * <p>The default value is {@link Integer#MAX_VALUE}.
     */
    public void setMaxMergeDocs(int maxMergeDocs) {
        this.maxMergeDocs = maxMergeDocs;
    }
    
    /**
     * @see #setMaxMergeDocs
     */
    public int getMaxMergeDocs() {
        return maxMergeDocs;
    }
    
    /**
     * The maximum number of terms that will be indexed for a single field in a
     * document.  This limits the amount of memory required for indexing, so that
     * collections with very large files will not crash the indexing process by
     * running out of memory.<p/>
     * Note that this effectively truncates large documents, excluding from the
     * index terms that occur further in the document.  If you know your source
     * documents are large, be sure to set this value high enough to accomodate
     * the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
     * is your memory, but you should anticipate an OutOfMemoryError.<p/>
     * By default, no more than 10,000 terms will be indexed for a field.
     */
    public void setMaxFieldLength(int maxFieldLength) {
        this.maxFieldLength = maxFieldLength;
    }
    
    /**
     * @see #setMaxFieldLength
     */
    public int getMaxFieldLength() {
        return maxFieldLength;
    }
    
    /** Determines the minimal number of documents required before the buffered
     * in-memory documents are merging and a new Segment is created.
     * Since Documents are merged in a {@link org.apache.lucene.store.RAMDirectory},
     * large value gives faster indexing.  At the same time, mergeFactor limits
     * the number of files open in a FSDirectory.
     *
     * <p> The default value is 10.
     * 
     * @throws IllegalArgumentException if maxBufferedDocs is smaller than 2
     */
    public void setMaxBufferedDocs(int maxBufferedDocs) {
        if (maxBufferedDocs < 2)
            throw new IllegalArgumentException("maxBufferedDocs must at least be 2");
        this.minMergeDocs = maxBufferedDocs;
    }
    
    /**
     * @see #setMaxBufferedDocs
     */
    public int getMaxBufferedDocs() {
        return minMergeDocs;
    }
    
    /** Determines how often segment indices are merged by addDocument().  With
     * smaller values, less RAM is used while indexing, and searches on
     * unoptimized indices are faster, but indexing speed is slower.  With larger
     * values, more RAM is used during indexing, and while searches on unoptimized
     * indices are slower, indexing is faster.  Thus larger values (> 10) are best
     * for batch index creation, and smaller values (< 10) for indices that are
     * interactively maintained.
     *
     * <p>This must never be less than 2.  The default value is 10.
     */
    public void setMergeFactor(int mergeFactor) {
        if (mergeFactor < 2)
            throw new IllegalArgumentException("mergeFactor cannot be less than 2");
        this.mergeFactor = mergeFactor;
    }
    
    /**
     * @see #setMergeFactor
     */
    public int getMergeFactor() {
        return mergeFactor;
    }
    
    /** If non-null, information about merges and a message when
     * maxFieldLength is reached will be printed to this.
     */
    public void setInfoStream(PrintStream infoStream) {
        this.infoStream = infoStream;
    }
    
    /**
     * @see #setInfoStream
     */
    public PrintStream getInfoStream() {
        return infoStream;
    }
    
    /** Flushes all changes to an index and closes all associated files. */
    public void close() throws IOException {
        flushRamSegments();
    }
    
    public void flushCache() throws Exception {
        writeCachedSegments();
    }
    
    /** Added since we moved segments to be stored in a db, we have to
      * separate out the post segment commit functionality to call it after we know
      * the transaction successfully committed.
      */
    public void finalizeTrans() throws IOException {
        updateIndexUponFinalize();
        ramDirectory.close();
        if(closeDir)
            directory.close();
    }
    
    /** Returns the Directory used by this index. */
    public Directory getDirectory() {
        return directory;
    }
    
    /** Returns the analyzer used by this index. */
    public Analyzer getAnalyzer() {
        return analyzer;
    }
    
    
    /** Returns the number of documents currently in this index. */
    public synchronized int docCount() {
        int count = 0;
        for (int i = 0; i < fsSegmentInfos.size(); i++) {
            count += fsSegmentInfos.info(i).docCount;
        }
        for (int i = 0; i < ramSegmentInfos.size(); i++) {
            count += ramSegmentInfos.info(i).docCount;
        }
        return count;
    }
    
    /**
     * The maximum number of terms that will be indexed for a single field in a
     * document.  This limits the amount of memory required for indexing, so that
     * collections with very large files will not crash the indexing process by
     * running out of memory.<p/>
     * Note that this effectively truncates large documents, excluding from the
     * index terms that occur further in the document.  If you know your source
     * documents are large, be sure to set this value high enough to accomodate
     * the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
     * is your memory, but you should anticipate an OutOfMemoryError.<p/>
     * By default, no more than 10,000 terms will be indexed for a field.
     * 
     * @deprecated use {@link #setMaxFieldLength} instead
     */
    public int maxFieldLength = DEFAULT_MAX_FIELD_LENGTH;
    
    /**
     * Adds a document to this index.  If the document contains more than
     * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
     * discarded.
     */
    public void addDocument(Document doc) throws IOException {
        addDocument(doc, analyzer);
    }
    
    /**
     * Adds a document to this index, using the provided analyzer instead of the
     * value of {@link #getAnalyzer()}.  If the document contains more than
     * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
     * discarded.
     */
    public void addDocument(Document doc, Analyzer analyzer) throws IOException {
        addDocumentToIndex(doc, analyzer);
    }
    
    /**
     * Adds a document to this index.  If the document contains more than
     * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
     * discarded.
     */
    public void addDocument(String sysid, Document doc) throws IOException {
        addDocument(sysid, doc, analyzer);
    }
    
    /**
     * Adds a document to this index, using the provided analyzer instead of the
     * value of {@link #getAnalyzer()}.  If the document contains more than
     * {@link #setMaxFieldLength(int)} terms for a given field, the remainder are
     * discarded.
     */
    public void addDocument(String sysid, Document doc, Analyzer analyzer) throws IOException {
        addDocumentToIndex(doc, analyzer);
        updatedDocuments.put(sysid, new Integer(addedDocumentCount));
    }
    
    private void addDocumentToIndex(Document doc, Analyzer analyzer) throws IOException {
        DocumentWriter dw =
            new DocumentWriter(ramDirectory, analyzer, this);
        //dw.setInfoStream(infoStream);
        String segmentName = newSegmentName(ramSegmentInfos);
        dw.addDocument(segmentName, doc);
        synchronized (this) {
            ramSegmentInfos.addElement(new SegmentInfo(segmentName, 1, ramDirectory));
            maybeMergeSegments();
        }
        addedDocumentCount++;
    }
    
    final int getSegmentsCounter(){
        return fsSegmentInfos.counter + ramSegmentInfos.counter;
    }
    
    protected static final String newSegmentName(SegmentInfos si) {
        synchronized (si) {
            return "_" + Integer.toString(si.counter++, Character.MAX_RADIX);
        }
    }
    
    /** Determines how often segment indices are merged by addDocument().  With
     * smaller values, less RAM is used while indexing, and searches on
     * unoptimized indices are faster, but indexing speed is slower.  With larger
     * values, more RAM is used during indexing, and while searches on unoptimized
     * indices are slower, indexing is faster.  Thus larger values (> 10) are best
     * for batch index creation, and smaller values (< 10) for indices that are
     * interactively maintained.
     *
     * <p>This must never be less than 2.  The default value is 10.
     * @deprecated use {@link #setMergeFactor} instead
     */
    public int mergeFactor = DEFAULT_MERGE_FACTOR;
    
    /** Determines the minimal number of documents required before the buffered
     * in-memory documents are merging and a new Segment is created.
     * Since Documents are merged in a {@link org.apache.lucene.store.RAMDirectory},
     * large value gives faster indexing.  At the same time, mergeFactor limits
     * the number of files open in a FSDirectory.
     *
     * <p> The default value is 10.
     * @deprecated use {@link #setMaxBufferedDocs} instead
     */
    public int minMergeDocs = DEFAULT_MIN_MERGE_DOCS;
    
    
    /** Determines the largest number of documents ever merged by addDocument().
     * Small values (e.g., less than 10,000) are best for interactive indexing,
     * as this limits the length of pauses while indexing to a few seconds.
     * Larger values are best for batched indexing and speedier searches.
     *
     * <p>The default value is {@link Integer#MAX_VALUE}.
     * @deprecated use {@link #setMaxMergeDocs} instead
     */
    public int maxMergeDocs = DEFAULT_MAX_MERGE_DOCS;
    
    /** If non-null, information about merges will be printed to this.
     * @deprecated use {@link #setInfoStream} instead 
     */
    public PrintStream infoStream = null;
    
    /** Not supporting this operation through the IndexWriter class anymore, use the 
     *  IndexOptimizer class instead
     */
    public synchronized void optimize() throws IOException {
    }
    
    public void delete(String sysid) throws IOException {
        DeletedInfos delInfos = IndexObjectsFactory.getDeletedInfos(this.directory);
        Integer intval = (Integer) delInfos.get(sysid);
        
        if (intval == null) {
            throw new IOException("Object '" + sysid + "' does not exist, so it can't be deleted");
        } 
        
        Document doc = createDeleteDocument(sysid);
        
        addDocumentToIndex(doc, analyzer);
 
        obsoleteDocuments.add(intval);
        deletedDocuments.add(new Integer(addedDocumentCount));
    }
    
    public boolean docExists(String sysid) throws IOException {
        DeletedInfos delInfos = IndexObjectsFactory.getDeletedInfos(this.directory);
        Integer intval = (Integer) delInfos.get(sysid);
        return intval != null;
    }
    
    public void update(String sysid, Document doc) throws IOException {
        this.update(sysid, doc, this.analyzer);
    }
    
    public void update(String sysid, Document doc, Analyzer a) throws IOException {
        DeletedInfos delInfos = IndexObjectsFactory.getDeletedInfos(this.directory);
        Integer intval = (Integer) delInfos.get(sysid);
        
        if (intval == null) {
            throw new IOException("Object '" + sysid + "' does not exist, so it cant be updated");
        } 
        
        addDocumentToIndex(doc, a);
        
        obsoleteDocuments.add(intval);
        updatedDocuments.put(sysid, new Integer(addedDocumentCount));
    }
    
    private static Document createDeleteDocument(String sysid) {
        Document doc = new Document();
        final int idx = sysid.indexOf(DeletedInfos.KEY_SEPERATOR);
        final int mode = Integer.parseInt(sysid.substring(idx + DeletedInfos.KEY_SEPERATOR.length()));
        sysid = sysid.substring(0, idx);
        doc.add(new Field(DeletedInfos.ID, sysid, Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(DeletedInfos.DELETED, DELETE_MARKER, Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field(DeletedInfos.LAYER, mode + "", Field.Store.YES, Field.Index.UN_TOKENIZED));
        return doc;
    }
    
    /** Merges all segments from an array of indexes into this index.
     *
     * <p>This may be used to parallelize batch indexing.  A large document
     * collection can be broken into sub-collections.  Each sub-collection can be
     * indexed in parallel, on a different thread, process or machine.  The
     * complete index can then be created by merging sub-collection indexes
     * with this method.ad
     *
     * <p>After this completes, the index is optimized. */
    public synchronized void addIndexes(Directory[] dirs)
    throws IOException {
        // not supporting this operation in our IndexWriter version, just does nothing
    }
    
    /** Merges the provided indexes into this index.
     * <p>After this completes, the index is optimized. </p>
     * <p>The provided IndexReaders are not closed.</p>
     */
    public synchronized void addIndexes(IndexReader[] readers)
    throws IOException {
        // not supporting this operation in our IndexWriter version, just does nothing
    }
    
    /** Merges all RAM-resident segments. */
    private final int flushRamSegments() throws IOException {
        int count = 0;
        if (ramSegmentInfos.size() > 0) {
            count = mergeSegments(ramSegmentInfos, 0);
        }
        return count;
    }
    
    /** Incremental segment merger.  */
    private final void maybeMergeSegments() throws IOException {
        long targetMergeDocs = minMergeDocs;
        while (targetMergeDocs <= maxMergeDocs) {
            // find segments smaller than current target size
            int minSegment = ramSegmentInfos.size();
            int mergeDocs = 0;
            while (--minSegment >= 0) {
                SegmentInfo si = ramSegmentInfos.info(minSegment);
                if (si.docCount >= targetMergeDocs) 
                    break;
                mergeDocs += si.docCount;
            }
            
            if (mergeDocs >= targetMergeDocs)		  // found a merge to do
                mergeSegments(ramSegmentInfos, minSegment+1);
            else
                break;
            
            targetMergeDocs *= mergeFactor;		  // increase target size
        }
    }
    
    /** Pops segments off of segmentInfos stack down to minSegment, merges them,
     and pushes the merged index onto the top of the segmentInfos stack. */
    private final int mergeSegments(SegmentInfos segmentInfos, int minSegment) throws IOException {
        return mergeSegments(segmentInfos, minSegment, segmentInfos.size()); 
    }
    
    /** Merges the named range of segments, replacing them in the stack with a
     * single segment. */
    private final int mergeSegments(SegmentInfos segmentInfos, int minSegment, int end) throws IOException {
        
        final String mergedName = newSegmentName(fsSegmentInfos);
        SegmentMerger merger = new SegmentMerger(this, mergedName);
        
        for (int i = minSegment; i < end; i++) {
            SegmentInfo si = segmentInfos.info(i);
            if (infoStream != null)
                infoStream.print(" " + si.name + " (" + si.docCount + " docs)");
            IndexReader reader = SegmentReader.get(si);
            merger.add(reader);
            if (reader.directory() == this.ramDirectory)
                segmentsToDelete.add(reader);   // queue segment for deletion
        }
        
        int mergedDocCount = merger.merge();
        
        if (infoStream != null) {
            infoStream.println(" into "+mergedName+" ("+mergedDocCount+" docs)");
        }
        
        for (int i = end-1; i >= minSegment; i--) {
            segmentInfos.remove(i);
        }
        tempSegmentInfos.addElement(new SegmentInfo(mergedName, mergedDocCount, directory));
        
        // close readers before we attempt to delete now-obsolete segments
        merger.closeReaders();
        
        segmentMergers.put(mergedName, merger);
        
        return mergedDocCount;
        
    }
    
    private final void writeCachedSegments() throws IOException {
        final int sizeOfTempSegments = tempSegmentInfos.size();
        
        synchronized (directory) {
            SegmentInfos tempFSSegmentInfos;
            synchronized (fsSegmentInfos) {
                tempFSSegmentInfos = fsSegmentInfos.copy();
            }
            for (int i = 0; i < sizeOfTempSegments; i++) {
                tempFSSegmentInfos.addElement(tempSegmentInfos.get(i));
            }
            tempFSSegmentInfos.write(directory);     // commit before deleting
//            IndexWriter.printSegmentInfos("IndexWriter, writing to segments.new: ", tempFSSegmentInfos);
        }
    }
    
    private final void updateIndexUponFinalize() throws IOException {
        final int sizeOfObsoleteDocs = obsoleteDocuments.size();
        final int sizeOfDeletedDocs = deletedDocuments.size();
        
        DeletedInfos delInfos = IndexObjectsFactory.getDeletedInfos(directory);
        
        synchronized (directory) {
            final int sizeOfTempSegments = tempSegmentInfos.size();
            synchronized (fsSegmentInfos) {
                for (int i = 0; i < sizeOfTempSegments; i++) {
                    fsSegmentInfos.addElement(tempSegmentInfos.remove(0));
                }
            }
            
            if (delInfos != null) {
                SynchronizedBitSet bitset = delInfos.getBitSet();
                final int docCount = delInfos.getDocCount();
                // now set the deleted bitset for all of the deleted documents
                for (int i = 0; i < sizeOfDeletedDocs; i++) {
                    bitset.set(((Integer) deletedDocuments.get(i)).intValue() + docCount);
                }

                for (int i = 0; i < sizeOfObsoleteDocs; i++) {
                    bitset.set(((Integer) obsoleteDocuments.get(i)).intValue());
                } 
                
                delInfos.setDocCount(docCount + addedDocumentCount + 1);
                
                Iterator iter = updatedDocuments.keySet().iterator();
                while (iter.hasNext()) {
                    String key = iter.next().toString();
                    Integer intval = (Integer) updatedDocuments.get(key);
                    delInfos.put(key, new Integer(intval.intValue() + docCount));
                }
            }
            
            obsoleteDocuments.clear();
            deletedDocuments.clear();
            updatedDocuments.clear();
            addedDocumentCount = 0;
            
            deleteSegments(directory, segmentsToDelete);  // delete now-unused segments
            
            if (useCompoundFile) {
                Iterator iter = segmentMergers.keySet().iterator();
                while (iter.hasNext()) {
                    String mergedName = iter.next().toString();
                    SegmentMerger merger = (SegmentMerger) segmentMergers.get(mergedName);
                    final Vector filesToDelete = merger.createCompoundFile(mergedName + ".tmp");
                    directory.renameFile(mergedName + ".tmp", mergedName + ".cfs");
                    // delete now unused files of segment 
                    deleteFiles(directory, filesToDelete);   
                }
            }
        }
        
        tempSegmentInfos.clear();
        tempSegmentInfos = null;
        segmentMergers.clear();
        segmentMergers = null;
    }
    
    public void onError() throws IOException {
        /*System.out.println("**** Inside onError *****");
        if (tempSegmentInfos == null) {
            return;
        }
        
        final int size = tempSegmentInfos.size();
        System.out.println("tempSegmentInfos.size() = " + size);
        for (int i = 0; i < size; i++) {
            SegmentInfo si = tempSegmentInfos.info(i);
            IndexReader reader = SegmentReader.get(si);
            System.out.println("si.name = " + si.name);
            segmentsToDelete.add(reader);   // queue segment for deletion
        }
        deleteSegments(directory, segmentsToDelete);  // delete now-unused segments*/
    }
    
    /*
     * Some operating systems (e.g. Windows) don't permit a file to be deleted
     * while it is opened for read (e.g. by another process or thread). So we
     * assume that when a delete fails it is because the file is open in another
     * process, and queue the file for subsequent deletion.
     */
    
    protected static final void deleteSegments(Directory directory, Vector segments) throws IOException { 
        Vector deletable = new Vector();
        
        deleteFiles(directory, readDeleteableFiles(directory), deletable); // try to delete deleteable
        
        for (int i = 0; i < segments.size(); i++) {
            SegmentReader reader = (SegmentReader)segments.elementAt(i);
            if (reader.directory() == directory)
                deleteFiles(directory, reader.files(), deletable);	  // try to delete our files
            else
                deleteFiles(reader.files(), reader.directory()); // delete other files
            
            //reader.close();
        }
        
        writeDeleteableFiles(directory, deletable);		  // note files we can't delete
    }
    
    protected static final void deleteFiles(Directory directory, Vector files) throws IOException { 
        Vector deletable = new Vector();
        deleteFiles(directory, readDeleteableFiles(directory), deletable); // try to delete deleteable
        deleteFiles(directory, files, deletable);     // try to delete our files
        writeDeleteableFiles(directory, deletable);        // note files we can't delete
    }
    
    private static final void deleteFiles(Vector files, Directory directory) throws IOException {
        for (int i = 0; i < files.size(); i++) 
            directory.deleteFile((String)files.elementAt(i));
    }
    
    private static final void deleteFiles(Directory directory, Vector files, Vector deletable) throws IOException {
        for (int i = 0; i < files.size(); i++) {
            String file = (String)files.elementAt(i);
            try {
                directory.deleteFile(file);		  // try to delete each file
            } catch (IOException e) {			  // if delete fails
                if (directory.fileExists(file)) {
                    deletable.addElement(file);		  // add to deletable
                }
            }
        }
    }
    
    private static final Vector readDeleteableFiles(Directory directory) throws IOException {
        Vector result = new Vector();
        if (!directory.fileExists(IndexFileNames.DELETABLE))
            return result;
        
        IndexInput input = directory.openInput(IndexFileNames.DELETABLE);
        try {
            for (int i = input.readInt(); i > 0; i--)	  // read file names
                result.addElement(input.readString());
        } finally {
            input.close();
        }
        return result;
    }
    
    private static final void writeDeleteableFiles(Directory directory, Vector files) throws IOException {
        IndexOutput output = directory.createOutput("deleteable.new");
        try {
            output.writeInt(files.size());
            for (int i = 0; i < files.size(); i++)
                output.writeString((String)files.elementAt(i));
        } finally {
            output.close();
        }
        directory.renameFile("deleteable.new", IndexFileNames.DELETABLE);
    }
    
    public SegmentInfosWrapper getSegmentInfosWrapper() {
        return new SegmentInfosWrapper(fsSegmentInfos);
    }
    
    public static void printSegmentInfos(String prolog, SegmentInfos infos) {
        System.out.print(prolog);
        for (int i = 0; i < infos.size(); i++) {
            if (i > 0) System.out.print(", " + infos.info(i).name);
            else System.out.print(infos.info(i).name);
        }
        System.out.println("");
    }
}
