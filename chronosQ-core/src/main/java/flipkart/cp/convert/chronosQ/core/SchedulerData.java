package flipkart.cp.convert.chronosQ.core;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;

import java.util.Optional;

@AllArgsConstructor
public class SchedulerData {
    private final String key;
    private String value;

    public String getKey() {
        return key;
    }

    public Optional<String> getValue() {
        if (Strings.isNullOrEmpty(value)) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    public SchedulerData(String key) {
        this.key = key;
    }
}
