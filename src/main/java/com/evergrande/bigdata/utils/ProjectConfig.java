package com.evergrande.bigdata.utils;

public class ProjectConfig {
    // Test Env
//    public static final String REDIS_HOST_IP = "59.212.226.7";
//    public static final int REDIS_HOST_PORT = 6379;

    // Production Env
    public static final String REDIS_HOST_IP = "59.212.226.49";
    public static final int REDIS_HOST_PORT = 6379;

    public static final int REDIS_MAX_TOTAL = 1024;
    public static final int REDIS_MAX_IDLE = 10000;
    public static final int REDIS_MAX_WAIT = 30000;
    public static final int REDIS_TIMEOUT = 30000;
    public static final boolean REDIS_TEST_ON_BORROW = true;
    public static final String REDIS_AUTH = "123456";

    public static final String KEY_PREFIX = "tt_test:";

    public final static String MOBILE_2_GUID_MAP = "mobile_to_guid_map_test";
    public final static String GUID_2_USER_MAP   = "guid_to_user_map_test";
}
