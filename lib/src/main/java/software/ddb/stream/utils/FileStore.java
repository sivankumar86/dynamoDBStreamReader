package software.ddb.stream.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Reading and writing to temp store for testing
 */
public class FileStore {

    public static void store(Map<String,String> input) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.convertValue(input, JsonNode.class);
        objectMapper.writeValue(new File("/tmp/mydata.json"), jsonNode);
    }


    public static Map<String, String> retrive() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(new File("/tmp/mydata.json"));
        Map<String, String> result = objectMapper.convertValue(jsonNode, new TypeReference<Map<String, String>>(){});
        return result;
    }
}
