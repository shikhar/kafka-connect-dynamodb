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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TaskConfig extends AbstractConfig {

    enum Keys {
        ;

        static String REGION = "region";
        static String TOPIC_FORMAT = "topic.format";
        static String SHARDS = "shards";
        static String TABLE = "table";
        static String STREAM_ARN = "stream.arn";
    }

    final Regions region;
    final String topicFormat;
    final List<String> shards;

    TaskConfig(Map<String, String> props) {
        super((Map) props);
        region = Regions.fromName(getString(Keys.REGION));
        topicFormat = getString(Keys.TOPIC_FORMAT);
        shards = Arrays.stream(getString(Keys.SHARDS).split(",")).filter(shardId -> !shardId.isEmpty()).collect(Collectors.toList());
    }

    String tableForShard(String shardId) {
        return getString(shardId + "." + Keys.TABLE);
    }

    String streamArnForShard(String shardId) {
        return getString(shardId + "." + Keys.STREAM_ARN);
    }

}
