package br.com.jeanpandolfi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {
    public static final String TYPE_CONFIG = "br.com.jeanpandolfi.type_config";

    private final Gson gson = new GsonBuilder().create();
    private Class<T> type;

    /** Métodp para pegar propriedades especificas de cada Deserializar a ser criado
     * Nesse caso é pega a property que diz isso no {@link KafkaService} e passada como parametro
     * no método deserialize para fazer o parse.*/
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String typeName = String.valueOf(configs.get(TYPE_CONFIG));
        try {
            this.type = (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Type for deserialization does not exist in the classpath", e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        return gson.fromJson(new String(bytes), type);
    }
}
