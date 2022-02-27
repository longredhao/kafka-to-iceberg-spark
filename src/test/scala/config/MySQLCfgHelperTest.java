package config;

import org.apache.iceberg.streaming.config.MySQLCfgHelper;
import org.apache.iceberg.streaming.config.TableCfg;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

public class MySQLCfgHelperTest {

    // @Test
    public void getConf() throws SQLException, IOException, ClassNotFoundException {
        String driver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/streaming?useSSL=false&serverTimezone=Asia/Shanghai";
        String user = "root";
        String password = "alex";
        String confKey = "B1%G1%tbl_test1^tbl_test2";
        String  andBy = "%";
        String  orBy = "\\^";
        ArrayList<TableCfg> tableCfgs = MySQLCfgHelper.getConf(driver, url, user, password, andBy, orBy, confKey);
        System.out.println(tableCfgs);
    }

    @Test
    public void orCase() {
        String columnName = "conf_id";
        String columnValue = "tbl_test1^tbl_test2";
        String splitBy="\\^";
        String result = MySQLCfgHelper.orCase(columnName,columnValue,splitBy);
        System.out.println(result);
        Assert.assertEquals(result, "(conf_id = 'tbl_test1' OR conf_id = 'tbl_test2')");
    }

    @Test
    public void getCondition() {
        String[] columnNames = new String[]{"batch_id", "group_id", "conf_id"};
        String columnValues = "B1%G1%tbl_test1^tbl_test2";
        String groupSplitBy = "%";
        String tableSplitBy = "\\^";
        String result = MySQLCfgHelper.getCondition(columnNames, columnValues, groupSplitBy, tableSplitBy);
        System.out.println(result);
        Assert.assertEquals(result, "((batch_id = 'B1') AND (group_id = 'G1') AND (conf_id = 'tbl_test1' OR conf_id = 'tbl_test2'))");
    }
}