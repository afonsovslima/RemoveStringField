package br.com.datastreambrasil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class RemoveCronogramaParcelas<R extends ConnectRecord<R>> implements Transformation<R> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public R apply(R record) {

        try {

            Object value = record.value();

            if (!(value instanceof Map)) {
                return record;
            }

            Map valueMap = (Map) value;

            Object afterObj = valueMap.get("after");

            if (!(afterObj instanceof Map)) {
                return record;
            }

            Map afterMap = (Map) afterObj;

            Object jsonField = afterMap.get("jsn_simulacao_motor");

            if (jsonField == null) {
                return record;
            }

            String jsonString = jsonField.toString();

            JsonNode node = mapper.readTree(jsonString);

            if (node.isObject()) {

                ObjectNode obj = (ObjectNode) node;

                if (obj.has("CronogramaParcelas")) {
                    obj.remove("CronogramaParcelas");
                }
            }

            afterMap.put("jsn_simulacao_motor", mapper.writeValueAsString(node));

        } catch (Exception e) {
            return record;
        }

        return record;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}