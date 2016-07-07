package com.evergrande.bigdata.utils;

public class ProjectConfig {
//    public static final String REDIS_HOST_IP = "120.26.215.198";
    public static final String REDIS_HOST_IP = "10.127.133.59";
    public static final int REDIS_HOST_PORT = 7777;
    public static final int REDIS_MAX_TOTAL = 1024;
    public static final int REDIS_MAX_IDLE = 10000;
    public static final int REDIS_MAX_WAIT = 30000;
    public static final int REDIS_TIMEOUT = 30000;
    public static final boolean REDIS_TEST_ON_BORROW = true;
    public static final String REDIS_AUTH = "O22nwf%Tt";

    public static final String KEY_PREFIX = "tt_test:";

    public final static String MOBILE_2_GUID_MAP = "mobile_to_guid_map_test";
    public final static String GUID_2_USER_MAP   = "guid_to_user_map_test";
}
