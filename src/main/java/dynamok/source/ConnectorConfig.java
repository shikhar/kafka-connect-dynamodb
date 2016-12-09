/*
 * Copyright 2016 Shikhar Bhushan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dynamok.source;

import com.amazonaws.regions.Regions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class ConnectorConfig extends AbstractConfig {

    private enum Keys {
        ;
        static final String REGION = "region";
        static final String ACCESS_KEY_ID = "access.key.id";
        static final String SECRET_KEY = "secret.key";
        static final String TABLES_PREFIX = "tables.prefix";
        static final String TABLES_WHITELIST = "tables.whitelist";
        static final String TABLES_BLACKLIST = "tables.blacklist";
        static final String TOPIC_FORMAT = "topic.format";
    }

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(Keys.REGION, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, (key, regionName) -> {
                if (Arrays.stream(Regions.values()).noneMatch(x -> x.getName().equals(regionName))) {
                    throw new ConfigException("Invalid AWS region: " + regionName);
                }
            }, ConfigDef.Importance.HIGH, "AWS region for DynamoDB.")
            .define(Keys.ACCESS_KEY_ID, ConfigDef.Type.PASSWORD, "",
                    ConfigDef.Importance.LOW, "Explicit AWS access key ID. " +
                            "Leave empty to utilize the default credential provider chain.")
            .define(Keys.SECRET_KEY, ConfigDef.Type.PASSWORD, "",
                    ConfigDef.Importance.LOW, "Explicit AWS secret access key. " +
                            "Leave empty to utilize the default credential provider chain.")
            .define(Keys.TABLES_PREFIX, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.MEDIUM, "Prefix for DynamoDB tables to source from.")
            .define(Keys.TABLES_WHITELIST, ConfigDef.Type.LIST, Collections.emptyList(),
                    ConfigDef.Importance.MEDIUM, "Whitelist for DynamoDB tables to source from.")
            .define(Keys.TABLES_BLACKLIST, ConfigDef.Type.LIST, Collections.emptyList(),
                    ConfigDef.Importance.MEDIUM, "Blacklist for DynamoDB tables to source from.")
            .define(Keys.TOPIC_FORMAT, ConfigDef.Type.STRING, "${table}",
                    ConfigDef.Importance.HIGH, "Format string for destination Kafka topic, use ``${table}`` as placeholder for source table name.");

    final Regions region;
    final Password accessKeyId;
    final Password secretKey;
    final String topicFormat;
    final String tablesPrefix;
    final List<String> tablesWhitelist;
    final List<String> tablesBlacklist;

    ConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        region = Regions.fromName(getString(Keys.REGION));
        accessKeyId = getPassword(Keys.ACCESS_KEY_ID);
        secretKey = getPassword(Keys.SECRET_KEY);
        tablesPrefix = getString(Keys.TABLES_PREFIX);
        tablesWhitelist = getList(Keys.TABLES_WHITELIST);
        tablesBlacklist = getList(Keys.TABLES_BLACKLIST);
        topicFormat = getString(Keys.TOPIC_FORMAT);
    }

    public static void main(String... args) {
        System.out.println(CONFIG_DEF.toRst());
    }

}
