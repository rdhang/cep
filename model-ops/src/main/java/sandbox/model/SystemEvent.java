package sandbox.model;


import lombok.Data;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import sandbox.util.BigNumberDeserializer;
import sandbox.util.CustomDateTimeDeserializer;

import java.time.LocalDateTime;
import java.util.Date;

@Data
@ToString
@JsonPropertyOrder({"timestamp", "pid", "user", "priority", "niceValue", "virtual", "physical",
                    "shared", "status", "cpuLoad", "memoryUsage", "command"})
public class SystemEvent {

    @JsonDeserialize(using = CustomDateTimeDeserializer.class)
    public Date timestamp;
    public int pid;
    public String user;
    public int priority;
    public int niceValue;

    @JsonDeserialize(using = BigNumberDeserializer.class)
    public long virtual;

    @JsonDeserialize(using = BigNumberDeserializer.class)
    public long physical;

    @JsonDeserialize(using = BigNumberDeserializer.class)
    public  long shared;

    public String status;
    public float cpuLoad;
    public float memoryUsage;
    public String command;


}
