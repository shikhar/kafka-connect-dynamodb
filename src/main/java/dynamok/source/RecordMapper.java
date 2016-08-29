package dynamok.source;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

public enum RecordMapper {
    ;

    private static final Schema AV_SCHEMA =
            SchemaBuilder.struct()
                    .name("DynamoDB.AttributeValue")
                    .field("S", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("N", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("B", Schema.OPTIONAL_BYTES_SCHEMA)
                    .field("SS", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                    .field("NS", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                    .field("BS", SchemaBuilder.array(Schema.BYTES_SCHEMA).optional().build())
                    .field("NULL", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                    .field("BOOL", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                    //      .field("L", "DynamoDB.AttributeValue") -- FIXME https://issues.apache.org/jira/browse/KAFKA-3910
                    //      .field("M", "DynamoDB.AttributeValue") -- FIXME https://issues.apache.org/jira/browse/KAFKA-3910
                    .version(1)
                    .build();

    private static final Schema DYNAMODB_ATTRIBUTES_SCHEMA =
            SchemaBuilder.map(Schema.STRING_SCHEMA, AV_SCHEMA)
                    .name("DynamoDB.Attributes")
                    .version(1)
                    .build();

    public static Schema attributesSchema() {
        return DYNAMODB_ATTRIBUTES_SCHEMA;
    }

    public static Map<String, Struct> toConnect(Map<String, AttributeValue> attributes) {
        Map<String, Struct> connectAttributes = new HashMap<>();
        for (Map.Entry<String, AttributeValue> attribute : attributes.entrySet()) {
            final String attributeName = attribute.getKey();
            final AttributeValue attributeValue = attribute.getValue();
            final Struct attributeValueStruct = new Struct(AV_SCHEMA);
            if (attributeValue.getS() != null) {
                attributeValueStruct.put("S", attributeValue.getS());
            } else if (attributeValue.getN() != null) {
                attributeValueStruct.put("N", attributeValue.getN());
            } else if (attributeValue.getB() != null) {
                attributeValueStruct.put("B", attributeValue.getB());
            } else if (attributeValue.getSS() != null) {
                attributeValueStruct.put("SS", attributeValue.getSS());
            } else if (attributeValue.getNS() != null) {
                attributeValueStruct.put("NS", attributeValue.getNS());
            } else if (attributeValue.getBS() != null) {
                attributeValueStruct.put("BS", attributeValue.getBS());
            } else if (attributeValue.getNULL() != null) {
                attributeValueStruct.put("NULL", attributeValue.getNULL());
            } else if (attributeValue.getBOOL() != null) {
                attributeValueStruct.put("BOOL", attributeValue.getBOOL());
            }
            connectAttributes.put(attributeName, attributeValueStruct);
        }
        return connectAttributes;
    }

}
