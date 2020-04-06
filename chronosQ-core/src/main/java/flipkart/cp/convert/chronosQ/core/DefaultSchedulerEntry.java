package flipkart.cp.convert.chronosQ.core;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class DefaultSchedulerEntry implements SchedulerEntry {

    private final String key;
    private String payload;

    public DefaultSchedulerEntry(String key) {
        this.key = key;
        this.payload = key; //backward compatible
    }

}
