package com.xjj.flink.function.cdas.util;



import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class JsonUtils {

    public static final Logger LOGGER = LoggerFactory.getLogger(JsonUtils.class);


    public static Map<String, Object> json2Map(String json) {

        ObjectMapper mapper = new ObjectMapper();

        try {

            Map<String, Object> map = mapper.readValue(json, Map.class);

            return map;
        } catch (Exception e) {
            LOGGER.error("",e);
        }
        return null;
    }

    public static Map<String, Object> json2Map(byte[] json) {

        ObjectMapper mapper = new ObjectMapper();

        try {

            Map<String, Object> map = mapper.readValue(json, Map.class);

            return map;
        } catch (Exception e) {
            LOGGER.error("",e);
        }
        return null;
    }
}
