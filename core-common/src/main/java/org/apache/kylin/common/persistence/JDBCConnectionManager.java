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

import com.google.common.base.Preconditions;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.EncryptUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JDBCConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(JDBCConnectionManager.class);

    private static JDBCConnectionManager INSTANCE = null;

<<<<<<< HEAD
    private static Object lock = new Object();

    public static JDBCConnectionManager getConnectionManager() {
        if (INSTANCE == null) {
            synchronized (lock) {
                if (INSTANCE == null) {
                    INSTANCE = new JDBCConnectionManager(KylinConfig.getInstanceFromEnv());
                }
            }
=======
    public static synchronized JDBCConnectionManager getConnectionManager() {
        if (INSTANCE == null) {
            INSTANCE = new JDBCConnectionManager(KylinConfig.getInstanceFromEnv());
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        }
        return INSTANCE;
    }

    // ============================================================================

    private final Map<String, String> dbcpProps;
    private final DataSource dataSource;

    private JDBCConnectionManager(KylinConfig config) {
        try {
            this.dbcpProps = initDbcpProps(config);

            dataSource = BasicDataSourceFactory.createDataSource(getDbcpProperties());
            Connection conn = getConn();
            DatabaseMetaData mdm = conn.getMetaData();
<<<<<<< HEAD
            logger.info("Connected to " + mdm.getDatabaseProductName() + " " + mdm.getDatabaseProductVersion());
            closeQuietly(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
=======
            logger.info("Connected to {0} {1}", mdm.getDatabaseProductName(), mdm.getDatabaseProductVersion());
            closeQuietly(conn);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        }
    }

    private Map<String, String> initDbcpProps(KylinConfig config) {
        // metadataUrl is like "kylin_default_instance@jdbc,url=jdbc:mysql://localhost:3306/kylin,username=root,password=xxx"
        StorageURL metadataUrl = config.getMetadataUrl();
        JDBCResourceStore.checkScheme(metadataUrl);

        LinkedHashMap<String, String> ret = new LinkedHashMap<>(metadataUrl.getAllParameters());
        List<String> mandatoryItems = Arrays.asList("url", "username", "password");

        for (String item : mandatoryItems) {
            Preconditions.checkNotNull(ret.get(item),
                    "Setting item \"" + item + "\" is mandatory for Jdbc connections.");
        }

        // Check whether password encrypted
        if ("true".equals(ret.get("passwordEncrypted"))) {
            String password = ret.get("password");
            ret.put("password", EncryptUtil.decrypt(password));
            ret.remove("passwordEncrypted");
        }

<<<<<<< HEAD
        logger.info("Connecting to Jdbc with url:" + ret.get("url") + " by user " + ret.get("username"));
=======
        logger.info("Connecting to Jdbc with url:{0} by user {1}", ret.get("url"), ret.get("username"));
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0

        putIfMissing(ret, "driverClassName", "com.mysql.jdbc.Driver");
        putIfMissing(ret, "maxActive", "5");
        putIfMissing(ret, "maxIdle", "5");
        putIfMissing(ret, "maxWait", "1000");
        putIfMissing(ret, "removeAbandoned", "true");
        putIfMissing(ret, "removeAbandonedTimeout", "180");
        putIfMissing(ret, "testOnBorrow", "true");
        putIfMissing(ret, "testWhileIdle", "true");
        putIfMissing(ret, "validationQuery", "select 1");
        return ret;
    }

    private void putIfMissing(LinkedHashMap<String, String> map, String key, String value) {
        if (map.containsKey(key) == false)
            map.put(key, value);
    }

    public final Connection getConn() throws SQLException {
        return dataSource.getConnection();
    }

    public Properties getDbcpProperties() {
        Properties ret = new Properties();
        ret.putAll(dbcpProps);
        return ret;
    }

    public static void closeQuietly(AutoCloseable obj) {
        if (obj != null) {
            try {
                obj.close();
            } catch (Exception e) {
                logger.warn("Error closing " + obj, e);
            }
        }
    }

    public void close() {
        try {
            ((org.apache.commons.dbcp.BasicDataSource) dataSource).close();
        } catch (SQLException e) {
            logger.error("error closing data source", e);
        }
    }
}
