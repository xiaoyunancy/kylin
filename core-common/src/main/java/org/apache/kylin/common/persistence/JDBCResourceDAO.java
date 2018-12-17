/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.persistence;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
<<<<<<< HEAD
import java.text.MessageFormat;
import java.util.List;
=======
import java.text.FieldPosition;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< HEAD
=======
import com.google.common.base.Preconditions;
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;

public class JDBCResourceDAO {

    private static Logger logger = LoggerFactory.getLogger(JDBCResourceDAO.class);

    private static final String META_TABLE_KEY = "META_TABLE_KEY";

    private static final String META_TABLE_TS = "META_TABLE_TS";

    private static final String META_TABLE_CONTENT = "META_TABLE_CONTENT";

    private JDBCConnectionManager connectionManager;

    private JDBCSqlQueryFormat jdbcSqlQueryFormat;

    private String[] tableNames = new String[2];

    private KylinConfig kylinConfig;

    // For test
    private long queriedSqlNum = 0;

    private FileSystem redirectFileSystem;

<<<<<<< HEAD
    public static final String LOG_TABLE_SUFFIX = "_log";
=======
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
    private Interner<String> interner = Interners.newStrongInterner();

    public JDBCResourceDAO(KylinConfig kylinConfig, String metadataIdentifier) throws SQLException {
        this.kylinConfig = kylinConfig;
        this.connectionManager = JDBCConnectionManager.getConnectionManager();
        this.jdbcSqlQueryFormat = JDBCSqlQueryFormatProvider
                .createJDBCSqlQueriesFormat(kylinConfig.getMetadataDialect());
        this.tableNames[0] = metadataIdentifier;
<<<<<<< HEAD
        this.tableNames[1] = metadataIdentifier + LOG_TABLE_SUFFIX;
=======
        this.tableNames[1] = metadataIdentifier + "_log";
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        for (int i = 0; i < tableNames.length; i++) {
            createTableIfNeeded(tableNames[i]);
            createIndex("IDX_" + META_TABLE_TS, tableNames[i], META_TABLE_TS);
        }
        try {
            redirectFileSystem = HadoopUtil.getReadFileSystem();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public void close() {
        connectionManager.close();
    }

    public JDBCResource getResource(final String resourcePath, final boolean fetchContent, final boolean fetchTimestamp)
            throws SQLException {
        return getResource(resourcePath, fetchContent, fetchTimestamp, false);
    }

    public JDBCResource getResource(final String resourcePath, final boolean fetchContent, final boolean fetchTimestamp,
            final boolean isAllowBroken) throws SQLException {
        final JDBCResource resource = new JDBCResource();
        logger.trace("getResource method. resourcePath : {} , fetchConetent : {} , fetch TS : {}", resourcePath,
                fetchContent, fetchTimestamp);
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                String tableName = getMetaTableName(resourcePath);
                pstat = connection.prepareStatement(getKeyEqualSqlString(tableName, fetchContent, fetchTimestamp));
                pstat.setString(1, resourcePath);
                rs = pstat.executeQuery();
                if (rs.next()) {
                    resource.setPath(rs.getString(META_TABLE_KEY));
                    if (fetchTimestamp)
                        resource.setTimestamp(rs.getLong(META_TABLE_TS));
                    if (fetchContent) {
                        try {
                            resource.setContent(getInputStream(resourcePath, rs));
<<<<<<< HEAD
                        } catch (Throwable e) {
=======
                        } catch (Exception e) {
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
                            if (!isAllowBroken) {
                                throw new SQLException(e);
                            }

                            final BrokenEntity brokenEntity = new BrokenEntity(resourcePath, e.getMessage());
                            resource.setContent(new BrokenInputStream(brokenEntity));
                            logger.warn(e.getMessage());
                        }
                    }
                }
            }
        });
        if (resource.getPath() != null) {
            return resource;
        } else {
            return null;
        }
    }

    public boolean existResource(final String resourcePath) throws SQLException {
        JDBCResource resource = getResource(resourcePath, false, false);
        return (resource != null);
    }

    public long getResourceTimestamp(final String resourcePath) throws SQLException {
        JDBCResource resource = getResource(resourcePath, false, true);
        return resource == null ? 0 : resource.getTimestamp();
    }

    //fetch primary key only
<<<<<<< HEAD
    public TreeSet<String> listAllResource(final String folderPath, final boolean recursive) throws SQLException {
        final TreeSet<String> allResourceName = new TreeSet<>();
=======
    public NavigableSet<String> listAllResource(final String folderPath, final boolean recursive) throws SQLException {
        final NavigableSet<String> allResourceName = new TreeSet<>();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        if (isRootPath(folderPath)) {
            for (int i = 0; i < tableNames.length; i++) {
                final String tableName = tableNames[i];
                listResource(tableName, folderPath, allResourceName, recursive);
            }
        } else {
            listResource(getMetaTableName(folderPath), folderPath, allResourceName, recursive);
        }
        return allResourceName;
    }

    private void listResource(final String tableName, final String folderPath,
<<<<<<< HEAD
                              final NavigableSet<String> allResourceName, final boolean recursive) throws SQLException {
=======
            final NavigableSet<String> allResourceName, final boolean recursive) throws SQLException {
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                pstat = connection.prepareStatement(getListResourceSqlString(tableName));
                pstat.setString(1, folderPath + "%");
                rs = pstat.executeQuery();
                while (rs.next()) {
                    String path = rs.getString(META_TABLE_KEY);
<<<<<<< HEAD
                    assert path.startsWith(folderPath);
=======
                    Preconditions.checkState(path.startsWith(folderPath));
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
                    if (recursive) {
                        allResourceName.add(path);
                    } else {
                        int cut = path.indexOf('/', folderPath.length());
                        String child = cut < 0 ? path : path.substring(0, cut);
                        allResourceName.add(child);
                    }
                }
            }
        });
    }

    public List<JDBCResource> getAllResource(final String folderPath, final long timeStart, final long timeEndExclusive,
            final boolean isAllowBroken) throws SQLException {
        final List<JDBCResource> allResource = Lists.newArrayList();
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                String tableName = getMetaTableName(folderPath);
                pstat = connection.prepareStatement(getAllResourceSqlString(tableName));
                pstat.setString(1, folderPath + "%");
                pstat.setLong(2, timeStart);
                pstat.setLong(3, timeEndExclusive);
                rs = pstat.executeQuery();
                while (rs.next()) {
                    String resPath = rs.getString(META_TABLE_KEY);
                    if (checkPath(folderPath, resPath)) {
                        JDBCResource resource = new JDBCResource();
                        resource.setPath(resPath);
                        resource.setTimestamp(rs.getLong(META_TABLE_TS));
                        try {
                            resource.setContent(getInputStream(resPath, rs));
<<<<<<< HEAD
                        } catch (Throwable e) {
=======
                        } catch (Exception e) {
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
                            if (!isAllowBroken) {
                                throw new SQLException(e);
                            }

                            final BrokenEntity brokenEntity = new BrokenEntity(resPath, e.getMessage());
                            resource.setContent(new BrokenInputStream(brokenEntity));
                            logger.warn(e.getMessage());
                        }
                        allResource.add(resource);
                    }
                }
            }
        });
        return allResource;
    }

    private boolean checkPath(String lookForPrefix, String resPath) {
        lookForPrefix = lookForPrefix.endsWith("/") ? lookForPrefix : lookForPrefix + "/";
        assert resPath.startsWith(lookForPrefix);
        int cut = resPath.indexOf('/', lookForPrefix.length());
        return (cut < 0);
    }

    private boolean isJsonMetadata(String resourcePath) {
        String trim = resourcePath.trim();
        return trim.endsWith(".json") || trim.startsWith(ResourceStore.EXECUTE_RESOURCE_ROOT)
                || trim.startsWith(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT);

    }

    public void deleteResource(final String resourcePath) throws SQLException {
        if (isRootPath(resourcePath)) {
            for (int i = 0; i < tableNames.length; i++) {
                final String tableName = tableNames[i];
                deleteResourceFromTable(tableName, resourcePath);
            }
        } else {
            String tableName = getMetaTableName(resourcePath);
            deleteResourceFromTable(tableName, resourcePath);
        }

        boolean skipHdfs = isJsonMetadata(resourcePath);
        if (!skipHdfs) {
            try {
                deleteHDFSResourceIfExist(resourcePath);
            } catch (Throwable e) {
                throw new SQLException(e);
            }
        }
    }

    private void deleteResourceFromTable(final String tableName, final String resourcePath) throws SQLException {
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                pstat = connection.prepareStatement(getDeletePstatSql(tableName));
                pstat.setString(1, resourcePath);
                pstat.executeUpdate();
            }
        });
<<<<<<< HEAD

    }
=======
    }

>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
    private void deleteHDFSResourceIfExist(String resourcePath) throws IOException {
        Path redirectPath = bigCellHDFSPath(resourcePath);
        if (redirectFileSystem.exists(redirectPath)) {
            redirectFileSystem.delete(redirectPath, true);
        }

    }

    public void putResource(final JDBCResource resource) throws SQLException {
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                byte[] content = getResourceDataBytes(resource);
                synchronized (interner.intern(resource.getPath())) {
                    boolean existing = existResource(resource.getPath());
                    String tableName = getMetaTableName(resource.getPath());
                    if (existing) {
                        pstat = connection.prepareStatement(getReplaceSql(tableName));
                        pstat.setLong(1, resource.getTimestamp());
                        pstat.setBlob(2, new BufferedInputStream(new ByteArrayInputStream(content)));
                        pstat.setString(3, resource.getPath());
                    } else {
                        pstat = connection.prepareStatement(getInsertSql(tableName));
                        pstat.setString(1, resource.getPath());
                        pstat.setLong(2, resource.getTimestamp());
                        pstat.setBlob(3, new BufferedInputStream(new ByteArrayInputStream(content)));
                    }

                    if (isContentOverflow(content, resource.getPath())) {
                        logger.debug("Overflow! resource path: {}, content size: {}, timeStamp: {}", resource.getPath(),
                                content.length, resource.getTimestamp());
                        if (existing) {
                            pstat.setNull(2, Types.BLOB);
                        } else {
                            pstat.setNull(3, Types.BLOB);
                        }
                        writeLargeCellToHdfs(resource.getPath(), content);
                        try {
                            int result = pstat.executeUpdate();
                            if (result != 1)
                                throw new SQLException();
                        } catch (SQLException e) {
                            rollbackLargeCellFromHdfs(resource.getPath());
                            throw e;
                        }
                        if (existing) {
                            cleanOldLargeCellFromHdfs(resource.getPath());
                        }
                    } else {
                        pstat.executeUpdate();
                    }
                }
            }
        });
    }

    public void checkAndPutResource(final String resPath, final byte[] content, final long oldTS, final long newTS)
            throws SQLException, WriteConflictException {
        logger.trace(
                "execute checkAndPutResource method. resPath : {} , oldTs : {} , newTs : {} , content null ? : {} ",
                resPath, oldTS, newTS, content == null);
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                synchronized (interner.intern(resPath)) {
                    String tableName = getMetaTableName(resPath);
                    if (!existResource(resPath)) {
                        if (oldTS != 0) {
                            throw new IllegalStateException(
                                    "For not exist file. OldTS have to be 0. but Actual oldTS is : " + oldTS);
                        }
                        if (isContentOverflow(content, resPath)) {
                            logger.debug("Overflow! resource path: {}, content size: {}", resPath, content.length);
                            pstat = connection.prepareStatement(getInsertSqlWithoutContent(tableName));
                            pstat.setString(1, resPath);
                            pstat.setLong(2, newTS);
                            writeLargeCellToHdfs(resPath, content);
                            try {
                                int result = pstat.executeUpdate();
                                if (result != 1)
                                    throw new SQLException();
                            } catch (SQLException e) {
                                rollbackLargeCellFromHdfs(resPath);
                                throw e;
                            }
                        } else {
                            pstat = connection.prepareStatement(getInsertSql(tableName));
                            pstat.setString(1, resPath);
                            pstat.setLong(2, newTS);
                            pstat.setBlob(3, new BufferedInputStream(new ByteArrayInputStream(content)));
                            pstat.executeUpdate();
                        }
                    } else {
                        // Note the checkAndPut trick:
                        // update {0} set {1}=? where {2}=? and {3}=?
                        pstat = connection.prepareStatement(getUpdateSqlWithoutContent(tableName));
                        pstat.setLong(1, newTS);
                        pstat.setString(2, resPath);
                        pstat.setLong(3, oldTS);
                        int result = pstat.executeUpdate();
                        if (result != 1) {
                            long realTime = getResourceTimestamp(resPath);
                            throw new WriteConflictException("Overwriting conflict " + resPath + ", expect old TS "
                                    + oldTS + ", but it is " + realTime);
                        }
                        PreparedStatement pstat2 = null;
                        try {
                            // "update {0} set {1}=? where {3}=?"
                            pstat2 = connection.prepareStatement(getUpdateContentSql(tableName));
                            if (isContentOverflow(content, resPath)) {
                                logger.debug("Overflow! resource path: {}, content size: {}", resPath, content.length);
                                pstat2.setNull(1, Types.BLOB);
                                pstat2.setString(2, resPath);
                                writeLargeCellToHdfs(resPath, content);
                                try {
                                    int result2 = pstat2.executeUpdate();
                                    if (result2 != 1)
                                        throw new SQLException();
                                } catch (SQLException e) {
                                    rollbackLargeCellFromHdfs(resPath);
                                    throw e;
                                }
                                cleanOldLargeCellFromHdfs(resPath);
                            } else {
                                pstat2.setBinaryStream(1, new BufferedInputStream(new ByteArrayInputStream(content)));
                                pstat2.setString(2, resPath);
                                pstat2.executeUpdate();
                            }
                        } finally {
                            JDBCConnectionManager.closeQuietly(pstat2);
                        }
                    }
                }
            }
        });
    }

    private byte[] getResourceDataBytes(JDBCResource resource) throws SQLException {
        ByteArrayOutputStream bout = null;
        try {
            bout = new ByteArrayOutputStream();
            IOUtils.copy(resource.getContent(), bout);
            return bout.toByteArray();
<<<<<<< HEAD
        } catch (Throwable e) {
=======
        } catch (Exception e) {
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
            throw new SQLException(e);
        } finally {
            IOUtils.closeQuietly(bout);
        }
    }

    private boolean isContentOverflow(byte[] content, String resPath) throws SQLException {
        if (kylinConfig.isJsonAlwaysSmallCell() && isJsonMetadata(resPath)) {

            int smallCellMetadataWarningThreshold = kylinConfig.getSmallCellMetadataWarningThreshold();
            int smallCellMetadataErrorThreshold = kylinConfig.getSmallCellMetadataErrorThreshold();

            if (content.length > smallCellMetadataWarningThreshold) {
                logger.warn(
                        "A JSON metadata entry's size is not supposed to exceed kylin.metadata.jdbc.small-cell-meta-size-warning-threshold("
                                + smallCellMetadataWarningThreshold + "), resPath: " + resPath + ", actual size: "
                                + content.length);
            }
            if (content.length > smallCellMetadataErrorThreshold) {
                throw new SQLException(new IllegalArgumentException(
                        "A JSON metadata entry's size is not supposed to exceed kylin.metadata.jdbc.small-cell-meta-size-error-threshold("
                                + smallCellMetadataErrorThreshold + "), resPath: " + resPath + ", actual size: "
                                + content.length));
            }

            return false;
        }

        int maxSize = kylinConfig.getJdbcResourceStoreMaxCellSize();
        if (content.length > maxSize)
            return true;
        else
            return false;
    }

    private void createTableIfNeeded(final String tableName) throws SQLException {
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                if (checkTableExists(tableName, connection)) {
                    logger.info("Table [{}] already exists", tableName);
                    return;
                }

                pstat = connection.prepareStatement(getCreateIfNeededSql(tableName));
                pstat.executeUpdate();
                logger.info("Create table [{}] success", tableName);
            }

            private boolean checkTableExists(final String tableName, final Connection connection) throws SQLException {
<<<<<<< HEAD
                final PreparedStatement ps = connection.prepareStatement(getCheckTableExistsSql(tableName));
                ResultSet rs = null;
                try {
=======
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = connection.prepareStatement(getCheckTableExistsSql(tableName));
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
                    rs = ps.executeQuery();
                    while (rs.next()) {
                        if (tableName.equals(rs.getString(1))) {
                            return true;
                        }
                    }
                } finally {
                    DBUtils.closeQuietly(rs);
                    DBUtils.closeQuietly(ps);
                }

                return false;
            }
        });
    }

    private void createIndex(final String indexName, final String tableName, final String colName) {
        try {
            executeSql(new SqlOperation() {
                @Override
                public void execute(Connection connection) throws SQLException {
                    pstat = connection.prepareStatement(getCreateIndexSql(indexName, tableName, colName));
                    pstat.executeUpdate();
                }
            });
        } catch (SQLException ex) {
            logger.info("Create index failed with message: " + ex.getLocalizedMessage());
        }
    }

    abstract static class SqlOperation {
        PreparedStatement pstat = null;
        ResultSet rs = null;

        abstract public void execute(final Connection connection) throws SQLException;
    }

    private void executeSql(SqlOperation operation) throws SQLException {
        Connection connection = null;
        try {
            connection = connectionManager.getConn();
            operation.execute(connection);
            queriedSqlNum++;
        } finally {
            JDBCConnectionManager.closeQuietly(operation.rs);
            JDBCConnectionManager.closeQuietly(operation.pstat);
            JDBCConnectionManager.closeQuietly(connection);
        }
    }

    private String getCheckTableExistsSql(final String tableName) {
<<<<<<< HEAD
        final String sql = MessageFormat.format(jdbcSqlQueryFormat.getCheckTableExistsSql(), tableName);
=======
        final String sql = new MessageFormat(jdbcSqlQueryFormat.getCheckTableExistsSql(), Locale.ROOT)
                .format(new Object[] { tableName }, new StringBuffer(), new FieldPosition(0)).toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    //sql queries
    private String getCreateIfNeededSql(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getCreateIfNeedSql(), tableName, META_TABLE_KEY,
                META_TABLE_TS, META_TABLE_CONTENT);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getCreateIfNeedSql(), Locale.ROOT)
                .format(new Object[] { tableName, META_TABLE_KEY, META_TABLE_TS, META_TABLE_CONTENT },
                        new StringBuffer(), new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    //sql queries
    private String getCreateIndexSql(String indexName, String tableName, String indexCol) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getCreateIndexSql(), indexName, tableName, indexCol);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getCreateIndexSql(), Locale.ROOT)
                .format(new Object[] { indexName, tableName, indexCol }, new StringBuffer(), new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getKeyEqualSqlString(String tableName, boolean fetchContent, boolean fetchTimestamp) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getKeyEqualsSql(),
                getSelectList(fetchContent, fetchTimestamp), tableName, META_TABLE_KEY);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getKeyEqualsSql(), Locale.ROOT)
                .format(new Object[] { getSelectList(fetchContent, fetchTimestamp), tableName, META_TABLE_KEY },
                        new StringBuffer(), new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getDeletePstatSql(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getDeletePstatSql(), tableName, META_TABLE_KEY);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getDeletePstatSql(), Locale.ROOT)
                .format(new Object[] { tableName, META_TABLE_KEY }, new StringBuffer(), new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getListResourceSqlString(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getListResourceSql(), META_TABLE_KEY, tableName,
                META_TABLE_KEY);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getListResourceSql(), Locale.ROOT)
                .format(new Object[] { META_TABLE_KEY, tableName, META_TABLE_KEY }, new StringBuffer(),
                        new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getAllResourceSqlString(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getAllResourceSql(), getSelectList(true, true), tableName,
                META_TABLE_KEY, META_TABLE_TS, META_TABLE_TS);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getAllResourceSql(), Locale.ROOT).format(
                new Object[] { getSelectList(true, true), tableName, META_TABLE_KEY, META_TABLE_TS, META_TABLE_TS },
                new StringBuffer(), new FieldPosition(0)).toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getReplaceSql(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getReplaceSql(), tableName, META_TABLE_TS,
                META_TABLE_CONTENT, META_TABLE_KEY);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getReplaceSql(), Locale.ROOT)
                .format(new Object[] { tableName, META_TABLE_TS, META_TABLE_CONTENT, META_TABLE_KEY },
                        new StringBuffer(), new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getInsertSql(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getInsertSql(), tableName, META_TABLE_KEY, META_TABLE_TS,
                META_TABLE_CONTENT);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getInsertSql(), Locale.ROOT)
                .format(new Object[] { tableName, META_TABLE_KEY, META_TABLE_TS, META_TABLE_CONTENT },
                        new StringBuffer(), new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    @SuppressWarnings("unused")
    private String getReplaceSqlWithoutContent(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getReplaceSqlWithoutContent(), tableName, META_TABLE_TS,
                META_TABLE_KEY);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getReplaceSqlWithoutContent(), Locale.ROOT)
                .format(new Object[] { tableName, META_TABLE_TS, META_TABLE_KEY }, new StringBuffer(),
                        new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getInsertSqlWithoutContent(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getInsertSqlWithoutContent(), tableName, META_TABLE_KEY,
                META_TABLE_TS);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getInsertSqlWithoutContent(), Locale.ROOT)
                .format(new Object[] { tableName, META_TABLE_KEY, META_TABLE_TS }, new StringBuffer(),
                        new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getUpdateSqlWithoutContent(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getUpdateSqlWithoutContent(), tableName, META_TABLE_TS,
                META_TABLE_KEY, META_TABLE_TS);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getUpdateSqlWithoutContent(), Locale.ROOT)
                .format(new Object[] { tableName, META_TABLE_TS, META_TABLE_KEY, META_TABLE_TS }, new StringBuffer(),
                        new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getUpdateContentSql(String tableName) {
<<<<<<< HEAD
        String sql = MessageFormat.format(jdbcSqlQueryFormat.getUpdateContentSql(), tableName, META_TABLE_CONTENT,
                META_TABLE_KEY);
=======
        String sql = new MessageFormat(jdbcSqlQueryFormat.getUpdateContentSql(), Locale.ROOT)
                .format(new Object[] { tableName, META_TABLE_CONTENT, META_TABLE_KEY }, new StringBuffer(),
                        new FieldPosition(0))
                .toString();
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return sql;
    }

    private String getSelectList(boolean fetchContent, boolean fetchTimestamp) {
        StringBuilder sb = new StringBuilder();
        sb.append(META_TABLE_KEY);
        if (fetchTimestamp)
            sb.append("," + META_TABLE_TS);
        if (fetchContent)
            sb.append("," + META_TABLE_CONTENT);
        return sb.toString();
    }

    private InputStream getInputStream(String resPath, ResultSet rs) throws SQLException, IOException {
        if (rs == null) {
            return null;
        }
        InputStream inputStream = rs.getBlob(META_TABLE_CONTENT) == null ? null
                : rs.getBlob(META_TABLE_CONTENT).getBinaryStream();
        if (inputStream != null) {
            return inputStream;
        } else {
            Path redirectPath = bigCellHDFSPath(resPath);
            return redirectFileSystem.open(redirectPath);
        }
    }

    private Path writeLargeCellToHdfs(String resPath, byte[] largeColumn) throws SQLException {

        boolean isResourceExist;
        FSDataOutputStream out = null;
        Path redirectPath = bigCellHDFSPath(resPath);
        Path oldPath = new Path(redirectPath.toString() + "_old");
        try {
            isResourceExist = redirectFileSystem.exists(redirectPath);
            if (isResourceExist) {
                FileUtil.copy(redirectFileSystem, redirectPath, redirectFileSystem, oldPath, false,
                        HadoopUtil.getCurrentConfiguration());
                redirectFileSystem.delete(redirectPath, true);
                logger.debug("a copy of hdfs file {} is made", redirectPath);
            }
            out = redirectFileSystem.create(redirectPath);
            out.write(largeColumn);
            return redirectPath;
<<<<<<< HEAD
        } catch (Throwable e) {
            try {
                rollbackLargeCellFromHdfs(resPath);
            } catch (Throwable ex) {
=======
        } catch (Exception e) {
            try {
                rollbackLargeCellFromHdfs(resPath);
            } catch (Exception ex) {
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
                logger.error("fail to roll back resource " + resPath + " in hdfs", ex);
            }
            throw new SQLException(e);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

<<<<<<< HEAD

=======
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
    public void rollbackLargeCellFromHdfs(String resPath) throws SQLException {
        Path redirectPath = bigCellHDFSPath(resPath);
        Path oldPath = new Path(redirectPath.toString() + "_old");
        try {
            if (redirectFileSystem.exists(oldPath)) {
                FileUtil.copy(redirectFileSystem, oldPath, redirectFileSystem, redirectPath, true, true,
                        HadoopUtil.getCurrentConfiguration());
                logger.info("roll back hdfs file {}", resPath);
            } else {
                redirectFileSystem.delete(redirectPath, true);
                logger.warn("no backup for hdfs file {} is found, clean it", resPath);
            }
<<<<<<< HEAD
        } catch (Throwable e) {
=======
        } catch (Exception e) {
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0

            try {
                //last try to delete redirectPath, because we prefer a deleted rather than incomplete
                redirectFileSystem.delete(redirectPath, true);
<<<<<<< HEAD
            } catch (Throwable ex) {
=======
            } catch (Exception ex) {
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
                logger.error("fail to delete resource " + redirectPath + " in hdfs", ex);
            }

            throw new SQLException(e);
        }
    }

    private void cleanOldLargeCellFromHdfs(String resPath) throws SQLException {
        Path redirectPath = bigCellHDFSPath(resPath);
        Path oldPath = new Path(redirectPath.toString() + "_old");
        try {
            if (redirectFileSystem.exists(oldPath)) {
                redirectFileSystem.delete(oldPath, true);
            }
<<<<<<< HEAD
        } catch (Throwable e) {
=======
        } catch (Exception e) {
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
            logger.warn("error cleaning the backup file for " + redirectPath + ", leave it as garbage", e);
        }
    }

    public Path bigCellHDFSPath(String resPath) {
        String hdfsWorkingDirectory = this.kylinConfig.getHdfsWorkingDirectory();
        Path redirectPath = new Path(hdfsWorkingDirectory, "resources-jdbc" + resPath);
<<<<<<< HEAD
        redirectPath =  Path.getPathWithoutSchemeAndAuthority(redirectPath);
=======
        redirectPath = Path.getPathWithoutSchemeAndAuthority(redirectPath);
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        return redirectPath;
    }

    public long getQueriedSqlNum() {
        return queriedSqlNum;
    }

    /**
     * Persist metadata to different SQL tables
     * @param resPath the metadata path key
     * @return the table name
     */
    public String getMetaTableName(String resPath) {
        if (isRootPath(resPath)) {
            throw new IllegalArgumentException("Not supported");
        }

        if (resPath.startsWith(ResourceStore.BAD_QUERY_RESOURCE_ROOT)
                || resPath.startsWith(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT)
                || resPath.startsWith(ResourceStore.TEMP_STATMENT_RESOURCE_ROOT)) {
            return tableNames[1];
        } else {
            return tableNames[0];
        }
    }

    public boolean isRootPath(String path) {
        return "/".equals(path);
    }

}