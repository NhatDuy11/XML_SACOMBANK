import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class MockYaml {
    public static void main(String[] args) throws Exception {

        Yaml yaml = new Yaml();
        try (InputStream in = new FileInputStream("D:\\JAVA\\convertFormatXML\\src\\main\\java\\table_ATMLOG.yaml")) {
            Map<String, Object> data = yaml.load(in);
            List<Map<String, Object>> tables = (List<Map<String, Object>>) ((List<Map<String, Object>>) data.get("database")).get(0).get("table");
            for (Map<String, Object> table : tables) {
                List<Map<String, Object>> columns = (List<Map<String, Object>>) table.get("columns");
                for (Map<String, Object> column : columns) {
                    String primaryKey = (String) column.get("primary_key");
                    String dataType = (String) column.get("data_type");
                    System.out.println("primary_key: " + primaryKey + ", data_type: " + dataType);
                }
            }
        }
    }
}
