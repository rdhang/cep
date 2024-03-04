package sandbox.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class BigNumberDeserializer extends JsonDeserializer<Long> {

    @Override
    public Long deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

        String value = jsonParser.getText();
        final float GB  = 1024L ^ 3;
        long result = 0L;
        if (value.contains("g")) {

            float incompleteValue = Float.parseFloat(value.replace("g", ""));
            result = (long)(incompleteValue * GB);

        } else {
            result = Long.parseLong(value);
        }

        return result;

    }
}
