package org.apache.iceberg.streaming.config;

import java.util.*;

/** Config. */
public class Config implements java.io.Serializable {

    private Map<String, String> config;
    private List<String> configNames;

    public Config() {}

    public static Config getInstance() {
        return new Config();
    }

    public static Config getInstance(Boolean init) {
        if (init) {
            Config config = new Config();
            config.config = new HashMap<>();
            config.configNames = new ArrayList<>();
            return config;
        } else {
            return getInstance();
        }
    }

    public Config loadConf(String[] confStrArr) {
        Map<String, String> config = new HashMap<>();
        List<String> configNames = new ArrayList<>(confStrArr.length);
        for (String confStr : confStrArr) {
            int splitIndex = confStr.indexOf("=");
            String key = confStr.substring(0, splitIndex).trim();
            String value = confStr.substring(splitIndex + 1).trim();
            if (!key.startsWith("#")) {
                config.put(key, value);
                configNames.add(key);
            }
        }
        this.config = config;
        this.configNames = configNames;
        return this;
    }

    public Config loadConf(String confText) {
        if (confText == null) return this;
        String[] confStrArr = confText.split("\n");
        Map<String, String> config = new LinkedHashMap<>();
        List<String> configNames = new ArrayList<>(confStrArr.length);
        for (String confStr : confStrArr) {
            if (confStr.contains("=")) {
                int splitIndex = confStr.indexOf("=");
                String key = confStr.substring(0, splitIndex).trim();
                String value = confStr.substring(splitIndex + 1).trim();
                if (!key.startsWith("#")) {
                    config.put(key, value);
                    configNames.add(key);
                }
            }
        }
        this.config = config;
        this.configNames = configNames;
        return this;
    }

    public Map<String, String> entrySet() {
        return this.config;
    }

    public boolean containsKey(String key) {
        return config.containsKey(key);
    }

    public void setConfig(String key, String value) {
        config.put(key, value);
        if (!configNames.contains(key)) {
            configNames.add(key);
        }
    }

    public String getStringValue(String key, String defaultValue) {
        return config.containsKey(key) ? String.valueOf(config.get(key)) : defaultValue;
    }

    public int getIntValue(String key, int defaultValue) {
        return config.containsKey(key) ? Integer.parseInt(config.get(key)) : defaultValue;
    }

    public long getLongValue(String key, long defaultValue) {
        return config.containsKey(key) ? Long.parseLong(config.get(key)) : defaultValue;
    }

    public double getDoubleValue(String key, double defaultValue) {
        return config.containsKey(key) ? Double.parseDouble(config.get(key)) : defaultValue;
    }

    public boolean getBooleanValue(String key, boolean defaultValue) {
        return config.containsKey(key) ? Boolean.parseBoolean(config.get(key)) : defaultValue;
    }

    public String getStringValue(String key) {
        return config.get(key);
    }

    public int getIntValue(String key) {
        return Integer.parseInt(config.get(key));
    }

    public long getLongValue(String key) {
        return Long.parseLong(config.get(key));
    }

    public double getDoubleValue(String key) {
        return Double.parseDouble(config.get(key));
    }

    public boolean getBooleanValue(String key) {
        return Boolean.parseBoolean(config.get(key));
    }

    @Override
    public String toString() {
        return "Config" + config;
    }

    public String toText() {
        StringBuilder buffer = new StringBuilder();
        for (String name : configNames) {
            buffer.append(name).append("=").append(config.get(name)).append("\n");
        }
        return buffer.toString();
    }

    public Properties toProperties(){
        Properties prop = new Properties();
        prop.putAll(config);
        return prop;
    }
}
