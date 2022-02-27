package config;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.iceberg.streaming.config.JobCfgHelper;
import org.apache.iceberg.streaming.config.TableCfg;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

public class JobCfgHelperTest {

    @org.junit.Test
    public void getConf() throws Exception {
        String runEnv = "test";
        String confKey = "mysql:B1%G1%tbl_test1^tbl_test2";
        ArrayList<TableCfg> confValue = JobCfgHelper.getInstance().getConf(confKey, runEnv);
        System.out.println(confValue);
    }

    @Test
    public void getUpdatedTextCfg() {
        String orgCfgText = "A=1\nB=2=b\nC=3";
        String k1="B";
        String v1="22=bb";
        String r1 = JobCfgHelper.getUpdatedTextCfg(orgCfgText, k1, v1);
        System.out.println(r1);
    }
}