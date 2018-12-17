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

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.util.HashMap;
<<<<<<< HEAD
=======
import java.util.Locale;
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
import java.util.Map;
import java.util.Properties;

public class JDBCSqlQueryFormatProvider {
    static Map<String, Properties> cache = new HashMap<>();

    public static JDBCSqlQueryFormat createJDBCSqlQueriesFormat(String dialect) {
<<<<<<< HEAD
        String key = String.format("/metadata-jdbc-%s.properties", dialect.toLowerCase());
=======
        String key = String.format(Locale.ROOT, "/metadata-jdbc-%s.properties", dialect.toLowerCase(Locale.ROOT));
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
        if (cache.containsKey(key)) {
            return new JDBCSqlQueryFormat(cache.get(key));
        } else {
            Properties props = new Properties();
            InputStream input = null;
            try {
                input = props.getClass().getResourceAsStream(key);
                props.load(input);
                if (!props.isEmpty()) {
                    cache.put(key, props);
                }
                return new JDBCSqlQueryFormat(props);
            } catch (Exception e) {
<<<<<<< HEAD
                throw new RuntimeException(String.format("Can't find properties named %s for metastore", key), e);
=======
                throw new RuntimeException(String.format(Locale.ROOT, "Can't find properties named %s for metastore", key), e);
>>>>>>> e8f96bb2534e07f8647215c1e878ec5af19399d0
            } finally {
                IOUtils.closeQuietly(input);
            }
        }

    }
}
