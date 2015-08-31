package flipkart.cp.convert.reservation.scheduler.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.cfgsvc.blogic.DProperty;
import com.flipkart.cfgsvc.client.CConfiguration;
import com.flipkart.cfgsvc.client.CScheduledUpdate;
import com.flipkart.cfgsvc.client.IConfiguration;
import com.flipkart.cp.cfgsvc.ConfigServiceClientWrapper;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.nio.charset.Charset.defaultCharset;


public class ConmanHelper {

    private static final Logger logger = LoggerFactory.getLogger(ConmanHelper.class);

    private static String CONFIG_FILE_EXTENSION = ".conf";

    private List<String> bucketNames;

    private ConfigServiceClientWrapper configServiceClientWrapper;

    private Map<String, String> propertyToJSONmap;

    public ConmanHelper(List<String> bucketNames) {
        this.bucketNames = bucketNames;
        configServiceClientWrapper = new ConfigServiceClientWrapper(bucketNames);
        propertyToJSONmap = new HashMap<String, String>();
    }


    public int getInteger(String property) {
        Integer value = null;
        try {
            value = configServiceClientWrapper.getInt(property);
        } catch (java.lang.ClassCastException e) {
            value = Integer.parseInt(configServiceClientWrapper.getString(property));
        }

        if (value == null) {
            throw new RuntimeException("Unable to find key: " + property );
        }

        return value;
    }


    public Long getLong(String property) {
        return new Long(getInteger(property));
    }

    public String getString(String property) {
        String value =  configServiceClientWrapper.getString(property);

        if (value == null) {
            throw new RuntimeException("Unable to find key: " + property );
        }

        return value;
    }


    public Boolean getBoolean(String property) {
        Boolean value = null;
        try {
            value = configServiceClientWrapper.getBoolean(property);
        } catch (java.lang.ClassCastException e) {
            value = Boolean.parseBoolean(configServiceClientWrapper.getString(property));
        }

        if (value == null) {
            throw new RuntimeException("Unable to find key: " + property );
        }

        return value;
    }

    public <T> T getObjectFromJSON(String property, Class<? extends T> klass) {

        String fileName = property + CONFIG_FILE_EXTENSION;

        String json = null;

        if (propertyToJSONmap.containsKey(property)) {

            json = propertyToJSONmap.get(property);

        } else {
            logger.info("Trying to open file: " + fileName) ;

            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(fileName)));
                String line;
                StringBuilder builder = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    builder.append(line);
                    builder.append("\n");
                }

                json = builder.toString();

                propertyToJSONmap.put(property, json);

                reader.close();

            } catch (Exception e) {
                throw new RuntimeException("Unable to read config file: " + fileName + " corresponding to property: " + property, e);
            }
        }

        ObjectMapper mapper = new ObjectMapper();

        T returnValue = null;
        try {
            returnValue = mapper.readValue(json, klass);
        } catch (Exception e) {
            throw new RuntimeException("Exception in parsing json from the file. JSON String: " + fileName, e);
        }

        return returnValue;
    }

}
