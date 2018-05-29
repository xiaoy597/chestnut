package com.evergrande.bigdata.utils;

import java.io.Serializable;

public class ProjectConfig implements Serializable{

    private static ProjectConfig projectConfig = null;

    public static ProjectConfig getInstance() {
        if (projectConfig == null) {
            projectConfig = new ProjectConfig();
            return projectConfig;
        } else return projectConfig;
    }

    public String REDIS_HOST_IP = null;
    public int REDIS_HOST_PORT = 0;

    public final int REDIS_MAX_TOTAL = 1024;
    public final int REDIS_MAX_IDLE = 10000;
    public final int REDIS_MAX_WAIT = 30000;
    public final int REDIS_TIMEOUT = 30000;
    public final boolean REDIS_TEST_ON_BORROW = true;
    public final String REDIS_AUTH = "123456";

    public String KEY_PREFIX = "";

    public final String MOBILE_2_GUID_MAP = "mobile_to_guid_map_test";
    public final String GUID_2_USER_MAP = "guid_to_user_map_test";
}
