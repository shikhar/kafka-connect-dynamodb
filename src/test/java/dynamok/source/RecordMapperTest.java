package dynamok.source;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RecordMapperTest {

    @Test
    public void conversions() {
        final String string = "test";
        final String number = "42";
        final ByteBuffer bytes = ByteBuffer.wrap(new byte[]{42});
        final boolean bool = true;
        final boolean nullValue = true;
        final Map<String, Struct> record = RecordMapper.toConnect(
                ImmutableMap.<String, AttributeValue>builder()
                        .put("thestring", new AttributeValue().withS(string))
                        .put("thenumber", new AttributeValue().withN(number))
                        .put("thebytes", new AttributeValue().withB(bytes))
                        .put("thestrings", new AttributeValue().withSS(string))
                        .put("thenumbers", new AttributeValue().withNS(number))
                        .put("thebyteslist", new AttributeValue().withBS(bytes))
                        .put("thenull", new AttributeValue().withNULL(true))
                        .put("thebool", new AttributeValue().withBOOL(bool))
                        .build()
        );
        assertEquals(string, record.get("thestring").get("S"));
        assertEquals(number, record.get("thenumber").get("N"));
        assertEquals(bytes, record.get("thebytes").get("B"));
        assertEquals(Collections.singletonList(string), record.get("thestrings").get("SS"));
        assertEquals(Collections.singletonList(number), record.get("thenumbers").get("NS"));
        assertEquals(Collections.singletonList(bytes), record.get("thebyteslist").get("BS"));
        assertEquals(nullValue, record.get("thenull").get("NULL"));
        assertEquals(bool, record.get("thebool").get("BOOL"));
    }

}
