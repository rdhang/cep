package home;

import lombok.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
public class Event {
    @JsonDeserialize(using = CustomDateDeserializer.class)
    public long date;
    public float temp;
    public float humidity;
    public float windspeed;
    public float pressure;


}
