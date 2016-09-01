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
