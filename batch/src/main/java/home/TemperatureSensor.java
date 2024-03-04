package home;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.ThreadLocalRandom;

public class TemperatureSensor extends RichSourceFunction<Sensor> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Sensor> sourceContext) throws Exception {
        while (this.running) {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            long timestamp = System.currentTimeMillis();
            Sensor sensor = new Sensor(random.nextInt(0, 10), random.nextDouble(50, 80), timestamp);
            sourceContext.collect(sensor);
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
