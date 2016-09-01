package dynamok.sink;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AttributeValueConverterTest {

    @Test
    public void schemalessConversion() {
        final Map<String, AttributeValue> attributeMap =
                AttributeValueConverter.toAttributeValueSchemaless(
                        ImmutableMap.builder()
                                .put("byte", (byte) 1)
                                .put("short", (short) 2)
                                .put("int", 3)
                                .put("long", 4L)
                                .put("float", 5.1f)
                                .put("double", 6.2d)
                                .put("decimal", new BigDecimal("7.3"))
                                .put("bool", true)
                                .put("string", "test")
                                .put("byte_array", new byte[]{42})
                                .put("byte_buffer", ByteBuffer.wrap(new byte[]{42}))
                                .put("list", Arrays.asList(1, 2, 3))
                                .put("empty_set", ImmutableSet.of())
                                .put("string_set", ImmutableSet.of("a", "b", "c"))
                                .put("number_set", ImmutableSet.of(1, 2, 3))
                                .put("bytes_set", ImmutableSet.of(new byte[]{42}))
                                .put("map", ImmutableMap.of("key", "value"))
                                .build()
                ).getM();
        assertEquals("1", attributeMap.get("byte").getN());
        assertEquals("2", attributeMap.get("short").getN());
        assertEquals("3", attributeMap.get("int").getN());
        assertEquals("4", attributeMap.get("long").getN());
        assertEquals("5.1", attributeMap.get("float").getN());
        assertEquals("6.2", attributeMap.get("double").getN());
        assertEquals("7.3", attributeMap.get("decimal").getN());
        assertTrue(attributeMap.get("bool").getBOOL());
        assertEquals("test", attributeMap.get("string").getS());
        assertEquals(ByteBuffer.wrap(new byte[]{42}), attributeMap.get("byte_array").getB());
        assertEquals(
                Arrays.asList(new AttributeValue().withN("1"), new AttributeValue().withN("2"), new AttributeValue().withN("3")),
                attributeMap.get("list").getL()
        );
        assertTrue(attributeMap.get("empty_set").getNULL());
        assertEquals(Arrays.asList("a", "b", "c"), attributeMap.get("string_set").getSS());
        assertEquals(Arrays.asList("1", "2", "3"), attributeMap.get("number_set").getNS());
        assertEquals(Arrays.asList(ByteBuffer.wrap(new byte[]{42})), attributeMap.get("bytes_set").getBS());
        assertEquals(ImmutableMap.of("key", new AttributeValue().withS("value")), attributeMap.get("map").getM());
    }

    @Test
    public void schemaedConversion() {
        Schema nestedStructSchema = SchemaBuilder.struct().field("x", SchemaBuilder.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct()
                .field("int8", SchemaBuilder.INT8_SCHEMA)
                .field("int16", SchemaBuilder.INT16_SCHEMA)
                .field("int32", SchemaBuilder.INT32_SCHEMA)
                .field("int64", SchemaBuilder.INT64_SCHEMA)
                .field("float32", SchemaBuilder.FLOAT32_SCHEMA)
                .field("float64", SchemaBuilder.FLOAT64_SCHEMA)
                .field("decimal", Decimal.schema(1))
                .field("bool", SchemaBuilder.BOOLEAN_SCHEMA)
                .field("string", SchemaBuilder.STRING_SCHEMA)
                .field("bytes_a", SchemaBuilder.BYTES_SCHEMA)
                .field("bytes_b", SchemaBuilder.BYTES_SCHEMA)
                .field("array", SchemaBuilder.array(SchemaBuilder.INT32_SCHEMA).build())
                .field("map", SchemaBuilder.map(SchemaBuilder.STRING_SCHEMA, SchemaBuilder.STRING_SCHEMA))
                .field("inner_struct", nestedStructSchema)
                .field("optional_string", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct struct = new Struct(schema)
                .put("int8", (byte) 1)
                .put("int16", (short) 2)
                .put("int32", 3)
                .put("int64", 4L)
                .put("float32", 5.1f)
                .put("float64", 6.2d)
                .put("decimal", new BigDecimal("7.3"))
                .put("bool", true)
                .put("string", "test")
                .put("bytes_a", new byte[]{42})
                .put("bytes_b", ByteBuffer.wrap(new byte[]{42}))
                .put("array", Arrays.asList(1, 2, 3))
                .put("map", ImmutableMap.of("key", "value"))
                .put("inner_struct", new Struct(nestedStructSchema).put("x", "y"));

        final Map<String, AttributeValue> attributeMap = AttributeValueConverter.toAttributeValue(schema, struct).getM();
        assertEquals("1", attributeMap.get("int8").getN());
        assertEquals("2", attributeMap.get("int16").getN());
        assertEquals("3", attributeMap.get("int32").getN());
        assertEquals("4", attributeMap.get("int64").getN());
        assertEquals("5.1", attributeMap.get("float32").getN());
        assertEquals("6.2", attributeMap.get("float64").getN());
        assertEquals("7.3", attributeMap.get("decimal").getN());
        assertTrue(attributeMap.get("bool").getBOOL());
        assertEquals("test", attributeMap.get("string").getS());
        assertEquals(ByteBuffer.wrap(new byte[]{42}), attributeMap.get("bytes_a").getB());
        assertEquals(ByteBuffer.wrap(new byte[]{42}), attributeMap.get("bytes_b").getB());
        assertEquals(
                Arrays.asList(new AttributeValue().withN("1"), new AttributeValue().withN("2"), new AttributeValue().withN("3")),
                attributeMap.get("array").getL()
        );
        assertEquals(ImmutableMap.of("key", new AttributeValue().withS("value")), attributeMap.get("map").getM());
        assertEquals(ImmutableMap.of("x", new AttributeValue().withS("y")), attributeMap.get("inner_struct").getM());
        assertTrue(attributeMap.get("optional_string").getNULL());
    }

}
