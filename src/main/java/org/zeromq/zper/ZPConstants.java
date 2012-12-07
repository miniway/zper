package org.zeromq.zper;

public class ZPConstants
{
    public static final String COMMAND_FETCH = "FETCH";
    public static final String COMMAND_OFFSET = "OFFSET";
    public static final String COMMAND_MASTER = "MASTER";
    
    public static final int STATUS_OK = 100;
    public static final int STATUS_INVALID_OFFSET = 101;
    public static final int STATUS_INVALID_COMMAND = 102;

    public static final int TYPE_RESPONSE = 1;
    public static final int TYPE_FILE = 2;

}
