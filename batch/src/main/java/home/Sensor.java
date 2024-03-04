package home;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
public class Sensor {

    private int deviceId;
    private double temp;
    private long timestamp;

}
