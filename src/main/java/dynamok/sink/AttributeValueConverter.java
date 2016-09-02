package dynamok.sink;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AttributeValueConverter {

    public static final AttributeValue NULL_VALUE = new AttributeValue().withNULL(true);

    public static AttributeValue toAttributeValue(Schema schema, Object value) {
        if (value == null) {
            if (schema.defaultValue() != null) {
                value = schema.defaultValue();
            } else if (schema.isOptional()) {
                return NULL_VALUE;
            } else {
                throw new ConnectException("null value for non-optional schema with no default value");
            }
        }

        if (schema.name() != null && schema.name().equals(Decimal.LOGICAL_NAME)) {
            return new AttributeValue().withN(value.toString());
        }

        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
                return new AttributeValue().withN(value.toString());
            case BOOLEAN:
                return new AttributeValue().withBOOL((boolean) value);
            case STRING:
                return new AttributeValue().withS((String) value);
            case BYTES:
                return new AttributeValue().withB(toByteBuffer(value));
            case ARRAY: {
                return new AttributeValue().withL(
                        ((List<?>) value).stream()
                                .map(item -> toAttributeValue(schema.valueSchema(), item))
                                .collect(Collectors.toList())
                );
            }
            case MAP: {
                if (schema.keySchema().isOptional()) {
                    throw new ConnectException("MAP key schema must not be optional");
                }
                if (!schema.keySchema().type().isPrimitive()) {
                    throw new ConnectException("MAP key schema must be of primitive type");
                }
                final Map<?, ?> sourceMap = (Map) value;
                final Map<String, AttributeValue> attributesMap = new HashMap<>(sourceMap.size());
                for (Map.Entry<?, ?> e : sourceMap.entrySet()) {
                    attributesMap.put(
                            primitiveAsString(e.getKey()),
                            toAttributeValue(schema.valueSchema(), e.getValue())
                    );
                }
                return new AttributeValue().withM(attributesMap);
            }
            case STRUCT: {
                final Struct struct = (Struct) value;
                final List<Field> fields = schema.fields();
                final Map<String, AttributeValue> attributesMap = new HashMap<>(fields.size());
                for (Field field : fields) {
                    attributesMap.put(field.name(), toAttributeValue(field.schema(), struct.get(field)));
                }
                return new AttributeValue().withM(attributesMap);
            }
            default:
                throw new ConnectException("Unknown Schema.Type: " + schema.type());
        }
    }

    public static AttributeValue toAttributeValueSchemaless(Object value) {
        if (value == null) {
            return NULL_VALUE;
        }
        if (value instanceof Number) {
            return new AttributeValue().withN(value.toString());
        }
        if (value instanceof Boolean) {
            return new AttributeValue().withBOOL((Boolean) value);
        }
        if (value instanceof String) {
            return new AttributeValue().withS((String) value);
        }
        if (value instanceof byte[] || value instanceof ByteBuffer) {
            return new AttributeValue().withB(toByteBuffer(value));
        }
        if (value instanceof List) {
            // We could have treated it as NS/BS/SS if the list is homogeneous and a compatible type, but can't know for ane empty list
            return new AttributeValue().withL(
                    ((List<?>) value).stream()
                            .map(AttributeValueConverter::toAttributeValueSchemaless)
                            .collect(Collectors.toList())
            );
        }
        if (value instanceof Set) {
            final Set<?> set = (Set) value;
            if (set.isEmpty()) {
                return NULL_VALUE;
            }
            final Object firstItem = ((Iterator) set.iterator()).next();
            if (firstItem instanceof String) {
                return new AttributeValue().withSS((Set<String>) set);
            }
            if (firstItem instanceof Number) {
                return new AttributeValue().withNS(set.stream().map(Object::toString).collect(Collectors.toSet()));
            }
            if (firstItem instanceof byte[] || firstItem instanceof ByteBuffer) {
                return new AttributeValue().withBS(set.stream().map(AttributeValueConverter::toByteBuffer).collect(Collectors.toSet()));
            }
            throw new ConnectException("Unsupported Set element type: " + firstItem.getClass());
        }
        if (value instanceof Map) {
            final Map<?, ?> sourceMap = (Map) value;
            final Map<String, AttributeValue> attributesMap = new HashMap<>(sourceMap.size());
            for (Map.Entry<?, ?> e : sourceMap.entrySet()) {
                attributesMap.put(
                        primitiveAsString(e.getKey()),
                        toAttributeValueSchemaless(e.getValue())
                );
            }
            return new AttributeValue().withM(attributesMap);
        }
        throw new ConnectException("Unsupported value type: " + value.getClass());
    }

    private static String primitiveAsString(Object value) {
        if (value instanceof Number || value instanceof Boolean || value instanceof String) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        } else if (value instanceof ByteBuffer) {
            return Base64.getEncoder().encode((ByteBuffer) value).asCharBuffer().toString();
        }
        throw new ConnectException("Not a primitive: " + value.getClass());
    }

    private static ByteBuffer toByteBuffer(Object bytesValue) {
        if (bytesValue instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) bytesValue);
        } else if (bytesValue instanceof ByteBuffer) {
            return ((ByteBuffer) bytesValue);
        } else {
            throw new ConnectException("Invalid bytes value of type: " + bytesValue.getClass());
        }
    }

}
