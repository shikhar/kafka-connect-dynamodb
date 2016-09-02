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

package dynamok.sink;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import java.util.List;
import java.util.Map;

public class UnprocessedItemsException extends Exception {

    public final Map<String, List<WriteRequest>> unprocessedItems;

    public UnprocessedItemsException(Map<String, List<WriteRequest>> unprocessedItems) {
        super(makeMessage(unprocessedItems));
        this.unprocessedItems = unprocessedItems;
    }

    private static String makeMessage(Map<String, List<WriteRequest>> unprocessedItems) {
        final StringBuilder msg = new StringBuilder("Unprocessed writes: {");
        for (Map.Entry<String, List<WriteRequest>> e : unprocessedItems.entrySet()) {
            msg.append(" ").append(e.getKey()).append("(").append(e.getValue().size()).append(")").append(" ");
        }
        msg.append("}");
        return msg.toString();
    }

}
