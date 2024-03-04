package sandbox.model;

import lombok.Data;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import sandbox.util.CustomDeserializer;

import java.time.LocalDate;

@Data
@ToString
@JsonPropertyOrder({"manufacturer", "model", "sales", "resaleValue", "vehicleType", "price", "engineSize",
                    "horsePower", "wheelbase", "width", "length", "curbWeight", "fuelCapacity", "fuelEfficiency",
                    "latestLaunch", "powerPerfFactor"})
public class Car {

    public String manufacturer;
    public String model;
    public double sales;
    public double resaleValue;
    public String vehicleType;
    public double price;
    public float engineSize;
    public int horsePower;
    public float wheelbase;
    public float width;
    public float length;
    public float curbWeight;
    public float fuelCapacity;
    public int fuelEfficiency;
    @JsonDeserialize(using = CustomDeserializer.class)
    public LocalDate latestLaunch;
    public double powerPerfFactor;
}
