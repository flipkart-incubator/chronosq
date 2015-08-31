package flipkart.cp.convert.chronosQ.example;


import flipkart.cp.convert.chronosQ.core.SchedulerEntry;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 10/02/15
 * Time: 6:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExampleSchedulerEntry implements SchedulerEntry {
    private String value;

    public ExampleSchedulerEntry(String value) {
        this.value = value;
    }

    @Override
    public String getStringValue() {
        return value;

    }
}
