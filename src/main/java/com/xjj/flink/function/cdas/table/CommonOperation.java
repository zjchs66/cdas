package com.xjj.flink.function.cdas.table;

import com.xjj.flink.function.cdas.entity.Operator;
import com.xjj.flink.function.cdas.util.JsonUtils;
import com.xjj.flink.function.cdas.util.TemporalConversions;
import org.apache.flink.table.data.RowData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author zhoujuncheng
 * @date 2022/6/20
 */
public interface CommonOperation {
//    default Object covertValue(String type, Object o) {
//        if (o == null || type == null) {
//            return o;
//        }
//        switch (type) {
//            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Time":
//                LocalTime localTime = TemporalConversions.toLocalTime(o);
//                String time = localTime.toString();
//                return time;
//            case "Date":
//                if (o instanceof String) {
//                    return Date.valueOf((String) o);
//                }
//                LocalDate localDate = TemporalConversions.toLocalDate(o);
//                return localDate.format(DateTimeFormatter.ISO_DATE);
//              //  return new Date(Date.valueOf(localDate).getTime());
////                return Date.valueOf(localDate);
//            case "TIMESTAMP":
//                Long timestamp = Long.valueOf(String.valueOf(o));
//                return new Timestamp(timestamp - 8 * 60 * 60 * 1000);
//            default:
//                return o;
//
//        }
//    }

    default Object covertValue(Operator op, Map.Entry<String, Object> entry) {
        Object o = entry.getValue();
        String type = op.getColumnsType().get(entry.getKey().toLowerCase());
        if (type == null) {
            return o;
        }
        switch (type) {
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Time":
                op.getColumnsType().put(entry.getKey().toLowerCase(), "String");
                if (o == null) {
                    return o;
                }
                Long l = 0L;
                if (o instanceof Integer) {
                    l = ((Integer) o).longValue();
                }
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
                simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT+0"));
                String t = simpleDateFormat.format(l);
                return t;
            case "Date":
                if (o == null) {
                    return o;
                }
                if (o instanceof String) {
                    return Date.valueOf((String) o);
                }

                LocalDate localDate = TemporalConversions.toLocalDate(o);
                return localDate.format(DateTimeFormatter.ISO_DATE);
            case "TIMESTAMP":
            case "DateTime64":
                if (o == null) {
                    return o;
                }
                Long timestamp = Long.valueOf(String.valueOf(o));
                SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                simpleDateFormat2.setTimeZone(TimeZone.getTimeZone("GMT+0"));
                String t2 = simpleDateFormat2.format(timestamp);
                return t2;
            default:
                if (type.contains("Decimal")) {
                    try {
                        if (o == null) {
                            return o;
                        }

                        int scale = 0;
                        String encoded = "";
                        if (o instanceof Map) {
                            scale = (Integer) ((Map) o).get("scale");
                            encoded = (String) ((Map) o).get("value");
                        } else {
                            encoded = (String) o;
                        }
                        final BigDecimal decoded = new BigDecimal(new BigInteger(Base64.getDecoder().decode(encoded)), scale);
                        return decoded;
                    } catch (Exception e) {
                        System.out.println(e);
                    }

                }
                return o;

        }
    }

    default String columnTypeMapper(String type, Map<String, Object> param) throws Exception {
        switch (type) {
            case "io.debezium.time.Date":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Date":
                return "Date";
            case "io.debezium.time.Time":
            case "io.debezium.time.MicroTime":
            case "io.debezium.time.NanoTime":
            case "io.debezium.time.Timestamp":
            case "io.debezium.time.MicroTimestamp":
            case "io.debezium.time.NanoTimestamp":
            case "com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Timestamp":
                return "TIMESTAMP";
            case "io.debezium.time.ZonedTime":
            default:
                return null;
        }
    }

    default Operator deserialize(RowData rowData) throws Exception {
        return deserialize(rowData.getString(0).toBytes());
    }

    default Operator deserialize(byte[] bytes) throws Exception {

        Operator sqlOperator = new Operator();
        if (bytes == null) {
            return null;
        }
        Map<String, Object> msg = JsonUtils.json2Map(bytes);

        if (msg != null) {
            for (String v : msg.keySet()) {
                if (v.equals("schema")) {
                    Map<String, Object> m = (Map<String, Object>) msg.get(v);
                    for (String v1 : m.keySet()) {
                        if (v1.equals("fields")) {
                            List<Map<String, Object>> m1 = (List<Map<String, Object>>) m.get(v1);
                            for (Map<String, Object> v2 : m1) {

                                List<Map<String, Object>> f = (List<Map<String, Object>>) v2.get("fields");
                                String key = (String) v2.get("field");
                                if (key.equals("after")) {
                                    for (Map<String, Object> f1 : f) {
                                        String fieldType = f1.containsKey("name") ? (String) f1.get("name") : (String) f1.get("type");
                                        Map<String, Object> param = f1.containsKey("parameters") ? (Map<String, Object>) f1.get("parameters") : null;
                                        sqlOperator.getColumnsType().put(((String) f1.get("field")).toLowerCase(), columnTypeMapper(fieldType, param));
                                    }
                                }

                                if (key.equals("costomColumns")) {
                                    for (Map<String, Object> f1 : f) {
                                        sqlOperator.getColumnsType().put("cus_" + f1.get("field"), columnTypeMapper((String) f1.get("type"), null));
                                    }
                                }

                            }

                        }
                    }
                }
                if (v.equals("payload")) {
                    Map<String, Object> m = (Map<String, Object>) msg.get(v);
                    for (String v1 : m.keySet()) {
                        Object o = m.get(v1);
                        if (o == null) {
                            continue;
                        }
                        if (v1.equals("key")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            for (Map.Entry<String, Object> entry : k.entrySet()) {
                                sqlOperator.getPr().put(entry.getKey().toLowerCase(), entry.getValue());
                            }

                        }

                        if (v1.equals("before")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            for (Map.Entry<String, Object> entry : k.entrySet()) {
                                //Object val = covertValue(sqlOperator.getColumnsType().get(entry.getKey().toLowerCase()), entry.getValue());
                                Object val = covertValue(sqlOperator, entry);
                                sqlOperator.getColumnsValue().put(entry.getKey().toLowerCase(), val);
                            }

                        }

                        if (v1.equals("after")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            for (Map.Entry<String, Object> entry : k.entrySet()) {
                                //Object val = covertValue(sqlOperator.getColumnsType().get(entry.getKey().toLowerCase()), entry.getValue());
                                Object val = covertValue(sqlOperator, entry);
                                sqlOperator.getColumnsValue().put(entry.getKey().toLowerCase(), val);
                            }

                        }
                        if (v1.equals("costomColumns")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            for (Map.Entry<String, Object> entry : k.entrySet()) {
                                sqlOperator.getColumnsValue().put("cus_" + entry.getKey(), entry.getValue());
                            }
                        }
                        if (v1.equals("source")) {
                            Map<String, Object> k = (Map<String, Object>) o;
                            sqlOperator.setDb((String) k.get("db"));
                            if (sqlOperator.getPr().size() < 1) {
                                sqlOperator.setTablename("npk_" + ((String) k.get("table")).toLowerCase());

                            } else {
                                sqlOperator.setTablename(((String) k.get("table")).toLowerCase());
                            }
                        }
                        if (v1.equals("op")) {
                            sqlOperator.setOpType((String) m.get("op"));
                        }
                        if (v1.equals("ts_ms")) {
                            sqlOperator.setOpts((Long) m.get("ts_ms"));
                        }

                    }
                }

            }

        }

        return sqlOperator;
    }
}
