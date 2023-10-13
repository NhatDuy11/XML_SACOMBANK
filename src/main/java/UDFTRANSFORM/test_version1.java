package UDFTRANSFORM;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.yaml.snakeyaml.Yaml;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;

import java.math.BigInteger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
@UdfDescription(name = "ConvertXmlUdf_experiment1", description = "Converts XML to target format")
public class test_version1 {
    public static void copyAttributeNode2Node(Node operationTag, Element insertRowElement) {

        NamedNodeMap attributes = operationTag.getAttributes();
        for (int i = 0; i < attributes.getLength(); i++) {
            Node attribute = attributes.item(i);
            String name = attribute.getNodeName();
            String value = name.equals("current_ts") ?
                    attribute.getNodeValue().replace("T", " ") :
                    attribute.getNodeValue();
            insertRowElement.setAttribute(name, value);
        }
    }
    public static String intenSeq() {
        String lUUID = String.format("%040d", new BigInteger(UUID.randomUUID().toString().replace("-", ""), 16));
        return lUUID;
    }

    @Udf(description = "Convert XML")
    public String convertXml(@UdfParameter(value = "xmlInput") String xmlInput, @UdfParameter(value = "Tablename") String tableName) throws Exception {
        try {
            String xmlinput_str = xmlInput.replace("\u001C", "");

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setCoalescing(true);
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            InputSource inputSource = new InputSource(new StringReader(xmlinput_str));
            Document doc = dBuilder.parse(inputSource);
            doc.getDocumentElement().normalize();

            NodeList operations = doc.getElementsByTagName("operation");
            Node operTag = operations.item(0);
            DocumentBuilderFactory targetFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder targetBuilder = targetFactory.newDocumentBuilder();
            Document targetDoc = targetBuilder.newDocument();
            Element operationElement = (Element) doc.getElementsByTagName("operation").item(0);
            System.out.println(operationElement);
//            String currentTs = operationElement.getAttribute("current_ts").replace("T", " ");
//            String ts = operationElement.getAttribute("ts");
//            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
//            LocalDateTime currentTsDateTime = LocalDateTime.parse(currentTs, formatter);
//            LocalDateTime tsDateTime = LocalDateTime.parse(ts, formatter);
//
//            double currentTsSeconds = currentTsDateTime.getSecond() + currentTsDateTime.getNano() / 1e9;
//            double tsSeconds = tsDateTime.getSecond() + tsDateTime.getNano() / 1e9;
//
//            double result = currentTsSeconds - tsSeconds;
//            System.out.println("result : " + result);
//
//            System.out.println(ts);
//            System.out.println(currentTs);

            Element insertRowElement = targetDoc.createElement("operation");

//            System.out.println("TS : " + ts);
            String rowOpAttr = intenSeq();

//            System.out.println(bigint);
            insertRowElement.setAttribute("intentSEQ", rowOpAttr);
//            double kafkprocess = result;
//            operationElement.setAttribute("processKafka", String.valueOf(kafkprocess));
            copyAttributeNode2Node(operTag, insertRowElement);
            targetDoc.appendChild(insertRowElement);
            NodeList colElements = doc.getElementsByTagName("col");
//            InputStream in = new FileInputStream("D:\\JAVA\\convertFormatXML\\src\\main\\java\\table_ATMLOG.yaml");
            InputStream in = new FileInputStream("/home/nhatduy/table_ATMLOG.yaml");

            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(in);
            List<Map<String, Object>> tables = (List<Map<String, Object>>) ((List<Map<String, Object>>) data.get("database"));
            for (int i = 0; i < colElements.getLength(); i++) {
                Element colElement = (Element) colElements.item(i);
                String colName = colElement.getAttribute("name");
                String colValue = colElement.getElementsByTagName("after").item(0).getTextContent();
                System.out.println(colValue);
                for (Map<String, Object> table : tables) {

                    Map<String, Object> tmpMapping = (Map<String, Object>) table;
                    Map<String, Object> tableMetadata = (Map<String, Object>) tmpMapping.get("table");

//                        System.out.println(tableMetadata.get("columns"));
                    String metaName = (String) tableMetadata.get("name");
//                    System.out.println(metaName);

                    if (tableName.equals(metaName)) {
                        System.out.println(tableName);
//                        System.out.println(String.format("Processing table %s", tableName));
                        String STATUS_TIME = "ATM_LOG";

                        List<Map<String, Object>> columns = (List<Map<String, Object>>) tableMetadata.get("columns");
                        for (Map<String, Object> column : columns) {
                            String columnName = (String) column.get("name");
                            System.out.println(columnName);
                            if (columnName.equals(colName)) {
                                Element targetColElement = targetDoc.createElement("col");
                                targetColElement.setAttribute("index", Integer.toString(i));
                                targetColElement.setAttribute("name", colName);
//

                                String primaryKey = (String) column.get("primary_key");
                                String dataType = (String) column.get("data_type");

                                Element dataTypeElementAFTER = targetDoc.createElement(dataType); // Tạo phần tử với tên là dataType
                                dataTypeElementAFTER.setTextContent(colValue);
                                targetColElement.appendChild(dataTypeElementAFTER);
                                targetColElement.setAttribute("PK", primaryKey);

                                insertRowElement.appendChild(targetColElement);
                            }
                        }
                    }
                }

            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(targetDoc), new StreamResult(writer));
            String convertedXml = writer.getBuffer().toString();
            return convertedXml;


        } catch (Exception e) {
            e.printStackTrace();
            throw  e ;
        }

    }


}



