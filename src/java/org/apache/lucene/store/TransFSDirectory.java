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
package org.apache.lucene.store;

import java.io.*;
import java.io.InputStream;
import java.sql.*;

import org.apache.lucene.index.IndexFileNames;

public class TransFSDirectory extends FSDirectory {

    public static final String SEGMENTS_NEW = "segments.new";
    
    protected String driverClass;
    protected String url;
    protected String user;
    protected String password;
    protected Connection conn = null;
    private boolean segmentsExists = false;

    public void finalize() throws Throwable {
        super.finalize();
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }
    
    /** Returns true iff a file with the given name exists. */
    public boolean fileExists(String name) {
        if (IndexFileNames.SEGMENTS.equals(name)) {
            if (!this.segmentsExists && this.segmentsExists()) {
                this.segmentsExists = true;
            }
            return this.segmentsExists;
        }
        return super.fileExists(name);
    }
    
    public boolean segmentsExists() {
        Connection conn = null;
        try {
            conn = getConnection();
            return segmentsExists(conn, getFile().getName());
        } catch (Exception ex) {
            return false;
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (SQLException sqle) { }
                conn = null;
            }
        }
    }
    
    public static boolean segmentsExists(Connection conn, final String dbHome) {
        boolean exists = false;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            final String sql = "SELECT segments FROM Lucene WHERE valid = ? AND db_home = ?";
            pstmt = conn.prepareStatement(sql);
            int count = 1;
            pstmt.setBoolean(count++, true);
            pstmt.setString(count++, dbHome);
            
            rs = pstmt.executeQuery();
            exists = rs.next();
        } catch (Exception ex) {
            exists = false;
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException ignore) { }
                rs = null;
            }
            if (pstmt != null) {
                try { pstmt.close(); } catch (SQLException ignore) { }
                pstmt = null;
            }
        }
        
        return exists;
    }
    
    public int getVersion() {
        Connection conn = null;
        try {
            conn = getConnection();
        } catch (Exception ex) {
            return -1;
        }
        return getVersion(conn, getFile().getName());
    }
    
    public static int getVersion(Connection conn, final String dbHome) {
        int version = -1;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            final String sql = "SELECT version FROM Lucene WHERE valid = ? AND db_home = ?";
            pstmt = conn.prepareStatement(sql);
            int count = 1;
            pstmt.setBoolean(count++, true);
            pstmt.setString(count++, dbHome);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                version = rs.getInt(1);
            }
        } catch (Exception ex) {
            version = -1;
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException ignore) { }
                rs = null;
            }
            if (pstmt != null) {
                try { pstmt.close(); } catch (SQLException ignore) { }
                pstmt = null;
            }
        }
        
        return version;
    }
    
    /** Returns the time the named file was last modified. */
    public long fileModified(String name) {
        if (IndexFileNames.SEGMENTS.equals(name)) {
            return this.segmentsModified();
        }
        return super.fileModified(name);
    }
    
    public long segmentsModified() {
        long modified = 0L;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            Connection conn = getConnection();
            
            final String sql = "SELECT timestamp FROM Lucene WHERE valid = ? AND db_home = ?";
            pstmt = conn.prepareStatement(sql);
            int count = 1;
            pstmt.setBoolean(count++, true);
            pstmt.setString(count++, getFile().getName());
            
            rs = pstmt.executeQuery();
            if (rs.next()) {
                modified = rs.getTimestamp(1).getTime();
            }
        } catch (Exception ex) {
            modified = 0L;
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException ignore) { }
                rs = null;
            }
            if (pstmt != null) {
                try { pstmt.close(); } catch (SQLException ignore) { }
                pstmt = null;
            }
        }
        
        return modified;
    }
    
    /** Set the modified time of an existing file to now. */
    public void touchFile(String name) {
        if (IndexFileNames.SEGMENTS.equals(name)) {
            this.touchSegments();
            return;
        }
        super.touchFile(name);
    }
    
    public void touchSegments() {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            Connection conn = getConnection();
            
            final String sql = "UPDATE Lucene SET valid = valid WHERE valid = ? AND db_home = ?";
            pstmt = conn.prepareStatement(sql);
            int count = 1;
            pstmt.setBoolean(count++, true);
            pstmt.setString(count++, getFile().getName());
            
            pstmt.executeUpdate();
        } catch (Exception ex) {
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException ignore) { }
                rs = null;
            }
            if (pstmt != null) {
                try { pstmt.close(); } catch (SQLException ignore) { }
                pstmt = null;
            }
        }
    }
    
    /** Returns the length in bytes of a file in the directory. */
    public long fileLength(String name) {
        if (IndexFileNames.SEGMENTS.equals(name)) {
            return this.segmentsLength();
        }
        return super.fileLength(name);
    }
    
    public long segmentsLength() {
        try {
            return new TransFSIndexInput(getConnection()).length;
        } catch (Exception ex) {
        }
        return 0L;
    }
    
    /** Removes an existing file in the directory. */
    public void deleteFile(String name, Connection conn) throws IOException {
        if (IndexFileNames.SEGMENTS.equals(name)) {
            this.deleteSegments();
            return;
        }
        super.deleteFile(name);
    }
    
    public void deleteSegments() throws IOException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        
        try {
            Connection conn = getConnection();
            
            final String sql = "UPDATE Lucene SET valid = ? WHERE db_home = ?";
            pstmt = conn.prepareStatement(sql);
            int count = 1;
            pstmt.setBoolean(count++, false);
            pstmt.setString(count++, getFile().getName());
            
            if (pstmt.executeUpdate() < 1) {
                throw new IOException("deleteSegments(), failed to mark the segment as invalid in the db");
            }
        } catch (Exception ex) {
            throw new IOException("deleteSegments(), failed to mark the segment as invalid in the db");
        } finally {
            if (rs != null) {
                try { rs.close(); } catch (SQLException ignore) { }
                rs = null;
            }
            if (pstmt != null) {
                try { pstmt.close(); } catch (SQLException ignore) { }
                pstmt = null;
            }
        }
    }
    
    /** Renames an existing file in the directory. */
    public synchronized void renameFile(String from, String to)
    throws IOException {
        if (SEGMENTS_NEW.equals(from) || IndexFileNames.SEGMENTS.equals(to)) {
            // renameTo() for the segments file does nothing now!
            // we are first storing segments.new in a relational db, then we will remove the file
            // from the file system.
        } else {
            super.renameFile(from, to);
        }
    }
    
    /** Returns a stream reading an existing file. */
    public IndexInput openInput(String name) throws IOException {
        if (IndexFileNames.SEGMENTS.equals(name)) {
            return new TransFSIndexInput(getConnection());
        }
        return super.openInput(name);
    }
    
    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }
    
    public void setUrl(String url) {
        this.url = url;
    }
    
    public void setUser(String user) {
        this.user = user;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    protected Connection getConnection() throws IOException {
        try {
            if (conn == null || conn.isClosed()) {
                Class.forName(driverClass);
                conn = DriverManager.getConnection(url, user, password);
            } else {
                Statement stmt = null;
                try {
                    // test the connection
                    stmt = conn.createStatement();
                    stmt.executeQuery("SELECT 1");
                } catch (SQLException sqle) {
                    conn.close();
                    // connection doesnt work, create a new one
                    Class.forName(driverClass);
                    conn = DriverManager.getConnection(url, user, password);
                } finally {
                    if (stmt != null) {
                        try { stmt.close(); } catch (Exception ignore) { }
                    } 
                }
            }
        } catch (Exception ex) {
            throw new IOException("TransFSDirectory.getConnection(): Could not create a connection to the segments db");
        }
        
        return conn;
    }
    
    public boolean isConnActive() throws SQLException {
        return (conn != null && !conn.isClosed());
    }
    
    
    class TransFSIndexInput extends BufferedIndexInput {
        
        byte[] segments = null;
        int position = 0;
        long length;
        
        public TransFSIndexInput(Connection conn) throws IOException {
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            
            try {
                String sql = "SELECT segments FROM Lucene WHERE valid = ? and db_home = ?";
                pstmt = conn.prepareStatement(sql);
                int count = 1;
                pstmt.setBoolean(count++, true);
                pstmt.setString(count++, getFile().getName());
                rs = pstmt.executeQuery();
                if (rs.next()) {
                	/* gs - blob to binary */
                    InputStream is = rs.getBinaryStream(1);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buf = new byte[4096];
                    int len;
                    while ((len = is.read(buf)) != -1) {
                      baos.write(buf, 0, len);
                    }
                    segments = baos.toByteArray();
                    length = segments.length;
                } 
                
                if (segments == null || length == 0L) {
                    throw new IOException("Segments doesn't exist in the database for " + getFile().getName());
                }
            } catch (Exception ex) {
                throw new IOException("TransFSIndexInput.init(): Could not retrieve the segments " +
                        "from the db, underlying exception message: " + ex.getMessage());
            } finally {
                if (rs != null) {
                    try { rs.close(); } catch (SQLException ignore) { }
                    rs = null;
                }
                if (pstmt != null) {
                    try { pstmt.close(); } catch (SQLException ignore) { }
                    pstmt = null;
                }
            }
        }
        
        /** IndexInput methods */
        protected void readInternal(byte[] b, int offset, int len)
        throws IOException {
            synchronized (segments) {
                if (position + len <= length) {
                    System.arraycopy(segments, position, b, offset, len);
                    position += len;
                } else {
                    throw new IOException("read past EOF");
                }
            }
        }
        
        public void close() throws IOException {
        }
        
        protected void seekInternal(long position) {
        }
        
        public long length() {
            return length;
        }
        
        public Object clone() {
            TransFSIndexInput clone = (TransFSIndexInput)super.clone();
            return clone;
        }
    }
    
}