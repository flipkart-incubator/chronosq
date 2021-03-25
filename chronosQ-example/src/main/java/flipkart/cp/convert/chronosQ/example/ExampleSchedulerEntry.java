package flipkart.cp.convert.chronosQ.example;


import flipkart.cp.convert.chronosQ.core.SchedulerEntry;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ExampleSchedulerEntry implements SchedulerEntry {

    private String value;
    private String payload;

    @Override
    public String getKey() {
        return value;

    }

    @Override
    public String getPayload() {
        return payload;
    }
}
