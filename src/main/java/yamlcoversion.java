
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.xml.XmlMapper;
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class yamlcoversion {
    public static void main(String[] args) {
        try {
            // 1st step - read YAML contents
            String yamlContent = new String(Files.readAllBytes(Paths.get(
                    "D:\\JAVA\\convertFormatXML\\src\\main\\java\\table_ATMLOG.yaml")));

            // 2nd step - convert YAML to XML using YAMLMapper and XmlMapper
            YAMLMapper yamlMapper = new YAMLMapper();
            XmlMapper xmlMapper = new XmlMapper();

            // Convert YAML to XML
            String xml = xmlMapper.writerWithDefaultPrettyPrinter() // pretty print
                    .writeValueAsString(yamlMapper.readValue(yamlContent, Object.class)); // Convert YAML to Java Object

            // Print XML to console
            System.out.println("YAML to XML conversion:\n\n" + xml);

            // You can also store the converted XML in a file here if needed

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method stores converted XML in Project class-path
     *
     * @param xml
     */




}
