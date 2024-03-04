package home;

import home.avro.model.Event;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class DataIngest {

    public static void main(String[] args) throws IOException {
        List<Event> events = new DataIngest().fetch();

        for (Event event : events) {
            System.out.println(event.toString());
        }
    }


    public List<Event> fetch() throws IOException {
        String[] HEADERS = {"id","name","value","status"};
        Reader in = new FileReader("events.csv");

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .setSkipHeaderRecord(true)
                .build();

        Iterable<CSVRecord> records = csvFormat.parse(in);

        Map<CharSequence, CharSequence> dataMap = null; //new HashMap<CharSequence, CharSequence>();
        dataMap.put("CPU_USAGE", "20.2");
        dataMap.put("STATUS", "RUNNING");

        List<Event> events = new ArrayList<>();
        for (CSVRecord record : records) {
            dataMap = new HashMap<CharSequence, CharSequence>();
            dataMap.put("VALUE", record.get("value"));
            dataMap.put("STATUS", record.get("status"));

            long now = LocalDateTime.now().atZone(ZoneId.of("America/New_York")).toInstant().toEpochMilli();
            Event event = Event.newBuilder()
                    .setId(UUID.randomUUID().toString())
                    .setName(record.get("name"))
                    .setTimestamp(now)
                    //.setData(dataMap)
                    .build();
            events.add(event);
        }
        return events;
    }

}
