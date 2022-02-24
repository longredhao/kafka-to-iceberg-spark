package org.apache.iceberg.streaming.config;

import java.io.IOException;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Properties;

public class TableCfg implements Serializable {
    private String id;
    private String batchId;
    private String groupId;
    private String confId;
    private String confValue;
    private String createTime;
    private String updateTime;

    public TableCfg() {}

    public TableCfg(String id, String batchId, String groupId, String confId, String confValue, String createTime, String updateTime) {
        this.id = id;
        this.batchId = batchId;
        this.groupId = groupId;
        this.confId = confId;
        this.confValue = confValue;
        this.createTime = createTime;
        this.updateTime = updateTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getConfId() {
        return confId;
    }

    public void setConfId(String confId) {
        this.confId = confId;
    }

    public String getConfValue() {
        return confValue;
    }

    public void setConfValue(String confValue) {
        this.confValue = confValue;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public Properties getCfgAsProperties() throws IOException {
        Properties prop = new Properties();
        prop.load(new StringReader(confValue));
        return prop;
    }
    @Override
    public String toString() {
        return "JobConfig{" +
                "id='" + id + '\'' +
                ", batchId='" + batchId + '\'' +
                ", groupId='" + groupId + '\'' +
                ", confId='" + confId + '\'' +
                ", confValue=" + confValue +
                ", createTime='" + createTime + '\'' +
                ", updateTime='" + updateTime + '\'' +
                '}';
    }

    public static final String JOB_CONF_LIB_INFO_FILE = "conf-address.properties";


}
