package flipkart.cp.convert.chronosQ.core;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SchedulerData {
    private final String key;
    private String value;

    public String getKey() {
        return key;
    }

    public String getValue() {
        if (Strings.isNullOrEmpty(value)) {
            return key;
        }
        return value;
    }

    public SchedulerData(String key) {
        this.key = key;
    }
}
