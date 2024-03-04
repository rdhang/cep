package sandbox.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import sandbox.model.SystemEvent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class JsonPojoDeserializer implements Deserializer<SystemEvent> {

    private ObjectMapper mapper;

    @Override
    public SystemEvent deserialize(String s, byte[] bytes) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        SystemEvent systemEvent = null;

        if (bytes != null) {
            try {
                systemEvent = mapper.readValue(bytes, SystemEvent.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return systemEvent;
    }



}
