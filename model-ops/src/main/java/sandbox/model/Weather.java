package sandbox.model;


import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

@Data
@ToString
@JsonPropertyOrder({"formattedDate", "summary","precipType","tempC","apparentTempC", "humidity",
                    "windSpeed","windBearing","visibility","loudCover","pressure", "dailySummary"})
public class Weather {

    //@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSSZ")
    public String formattedDate;
    public String summary;
    public String precipType;
    public String tempC;
    public String apparentTempC;
    public String humidity;
    public String windSpeed;
    public String windBearing;
    public String visibility;
    public String loudCover;
    public String pressure;
    public String dailySummary;

}
