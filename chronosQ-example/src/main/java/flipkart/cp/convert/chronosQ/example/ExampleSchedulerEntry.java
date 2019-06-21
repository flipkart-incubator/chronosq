package flipkart.cp.convert.chronosQ.example;


import flipkart.cp.convert.chronosQ.core.SchedulerEntry;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ExampleSchedulerEntry implements SchedulerEntry {

    private String value;

    @Override
    public String getStringValue() {
        return value;

    }
}
