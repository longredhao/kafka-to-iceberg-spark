package org.apache.iceberg.streaming.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

/** Get job conf from configure center. */
public class JobCfgHelper {
    private static final Logger logger = LoggerFactory.getLogger(JobCfgHelper.class);

    private static final String SELECT_CONDITION_AND_SPLIT_BY = "select.condition.and.splitBy";
    private static final String SELECT_CONDITION_OR_SPLIT_BY = "select.condition.or.splitBy";


    private static final String MYSQL_JDBC_URL = "mysql.jdbc.url";
    private static final String MYSQL_JDBC_DRIVER = "mysql.jdbc.driver";
    private static final String MYSQL_LOGIN_USER = "mysql.login.user";
    private static final String MYSQL_LOGIN_PASSWORD = "mysql.login.password";

    private static final String TEST_MYSQL_JDBC_URL = "test.mysql.jdbc.url";
    private static final String TEST_MYSQL_JDBC_DRIVER = "test.mysql.jdbc.driver";
    private static final String TEST_MYSQL_LOGIN_USER = "test.mysql.login.user";
    private static final String TEST_MYSQL_LOGIN_PASSWORD = "test.mysql.login.password";


    public static JobCfgHelper getInstance() {
        return new JobCfgHelper();
    }


    /**
     * 从远程配置服务器读取作业运行配置信息.
     *
     * @param confKey 配置的Key, 格式：配置库类型:配置在配置库中的索引ID.
     * @param props 远程访问配置库的地址信息参数.
     * @param useCfg 用户输入的远程访问配置库的地址信息参数.
     * @return JobRunningConf 作业运行配置，格式 JsonNode.
     * @throws IOException IOException
     * @throws SQLException SQLException
     * @throws ClassNotFoundException Exception
     */
    public ArrayList<TableCfg> getConf(String confKey, Properties props, Properties useCfg)
            throws IOException, SQLException, ClassNotFoundException {
        logger.info("JobConfLoader load conf, confKey[" + confKey + "]");
        int splitIndex = confKey.indexOf(":");
        String type = confKey.substring(0, splitIndex);
        String key = confKey.substring(splitIndex + 1);

        String runEnv = useCfg.getProperty("runEnv", "product");

        /* 使用 switch 处理不同的配置库类型. */
        switch (type) {
            case "mysql":
                {
                    if ("product".equals(runEnv)) {
                        ArrayList<TableCfg> tableCfgs =
                                MySQLCfgHelper.getConf(
                                        props.getProperty(MYSQL_JDBC_DRIVER),
                                        props.getProperty(MYSQL_JDBC_URL),
                                        props.getProperty(MYSQL_LOGIN_USER),
                                        props.getProperty(MYSQL_LOGIN_PASSWORD),
                                        props.getProperty(SELECT_CONDITION_AND_SPLIT_BY),
                                        props.getProperty(SELECT_CONDITION_OR_SPLIT_BY),
                                        key);
                        return tableCfgs;
                    } else if ("test".equals(runEnv)) {
                        ArrayList<TableCfg> tableCfgs =
                                MySQLCfgHelper.getConf(
                                        props.getProperty(TEST_MYSQL_JDBC_DRIVER),
                                        props.getProperty(TEST_MYSQL_JDBC_URL),
                                        props.getProperty(TEST_MYSQL_LOGIN_USER),
                                        props.getProperty(TEST_MYSQL_LOGIN_PASSWORD),
                                        props.getProperty(SELECT_CONDITION_AND_SPLIT_BY),
                                        props.getProperty(SELECT_CONDITION_OR_SPLIT_BY),
                                        key);
                        return tableCfgs;
                    } else {
                        throw new IllegalArgumentException("Unknown runEnv: " + runEnv);
                    }
                }
            default:
                {
                    logger.error("Unknown conf type: " + type);
                }
                throw new IllegalArgumentException("Unknown conf type: " + type);
        }
    }

    public ArrayList<TableCfg> getConf(String confKey, Properties useCfg) throws Exception {
        Properties confAddressProps = new Properties();
        confAddressProps.load(JobCfgHelper.class.getClassLoader().getResourceAsStream(TableCfg.JOB_CONF_LIB_INFO_FILE));
        return getConf(confKey, confAddressProps, useCfg);
    }


    /**
     * 更新 Text String 配置文件里指定配置的值
     * @param orgCfgText 原始Text String 配置
     * @param key 待修改配置的key
     * @param newValue 待修改配置的新值
     * @return
     */
    public static String getUpdatedTextCfg(String orgCfgText, String key, String newValue) {
        try (BufferedReader reader = new BufferedReader(new StringReader(orgCfgText))) {
            StringBuilder result = new StringBuilder();

            String line = reader.readLine();
            while (line != null) {
                if(line.trim().split("=")[0].trim().equals(key)){
                    String newCfg = String.format("%s=%s\n", key, newValue);
                    result.append(newCfg);
                }else{
                    result.append(line).append("\n");
                }
                line = reader.readLine();
            }
            return result.toString();
        } catch (IOException exc) {
           throw new RuntimeException("get updated text config value error...");
        }
    }
}
