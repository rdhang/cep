package home;

import home.avro.model.Event;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class ScheduledPublisher {

    public static void main(String[] args) throws IOException {

        String fileName = "./test.csv";

        Stream<Event> stream = Files.lines(Paths.get(fileName))
                .map(l -> {
                    String[] fields = l.split(",");

                    LocalDateTime localDateTime = LocalDateTime.parse(fields[2],
                            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS") );

                    long millis = localDateTime
                            .atZone(ZoneId.systemDefault())
                            .toInstant().toEpochMilli();

                    Map<String, String> dataMap = new HashMap<>();
                    dataMap.put("VALUE", fields[3]);

                    return Event.newBuilder()
                            .setId(fields[0])
                            .setName(fields[1])
                            .setTimestamp(millis)
                            .setData(dataMap)
                            .build();
                })
                .sorted(Comparator.comparing(Event::getTimestamp));

        stream.forEach(System.out::println);
    }

}
