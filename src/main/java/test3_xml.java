import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.yaml.snakeyaml.Yaml;

import javax.xml.parsers.*;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class test3_xml {


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
    public static Map<String, Object> data;






    public static void main(String[] args) {
        try {

            long startTime = System.nanoTime();





            //String xmlInputATMLog = "<?xml version='1.0' encoding='UTF-8'?>\n<operation table='SACOM_SW_OWN.ATM_LOG' type='I' ts='2023-08-22 13:49:52.364222' current_ts='2023-08-22T13:50:00.210000' pos='00000000020001362579' numCols='8'>\n <col name='SHCLOG_ID' index='0'>\n  <before missing='true'/>\n <after><![CDATA[AAEAsgAkZN52WQAB]]></after>\n </col>\n <col name='INSTITUTION_ID' index='1'>\n  <before missing='true'/>\n <after><![CDATA[1]]></after>\n </col>\n <col name='GROUP_NAME' index='2'>\n<before missing='true'/>\n<after><![CDATA[CDM5020105]]></after>\n </col>\n <col name='UNIT' index='3'>\n <before missing='true'/>\n  <after><![CDATA[22]]></after>\n </col>\n <col name='FUNCTION_CODE' index='4'>\n <before missing='true'/>\n<after><![CDATA[200]]></after>\n </col>\n <col name='LOGGED_TIME' index='5'>\n  <before missing='true'/>\n <after><![CDATA[2023-08-18 02:34:49.000000000]]></after>\n </col>\n <col name='LOG_DATA' index='6'>\n  <before missing='true'/>\n  <after><![CDATA[22\u001c022\u001c\u001c9]]></after>\n </col>\n <col name='SITE_ID' index='7'>\n  <before missing='true'/>\n<after><![CDATA[1]]></after>\n </col>\n</operation>\n";
            String XmlInputATMlog = ReadFile.readFile("ATM_LOG.txt", StandardCharsets.UTF_8).replace("\u001C", "");
//            Pattern pattern = Pattern.compile(".*current_ts='\\d{4}-\\d{2}-\\d{2}T(\\d{2}:\\d{2}:\\d{2}\\.\\d{6})'.*ts='\\d{4}-\\d{2}-\\d{2} (\\d{2}:\\d{2}:\\d{2}\\.\\d{6})'.*");
//            Matcher matcher = pattern.matcher(XmlInputATMlog);
//            if (matcher.matches()){
//                String currentTsSecondsStr = matcher.group(1);
//                String tsSecondsStr = matcher.group(2);
//                double currentTsSeconds = Double.parseDouble(currentTsSecondsStr);
//                double tsSeconds = Double.parseDouble(tsSecondsStr);
//
//                double difference = currentTsSeconds - tsSeconds;
//
//
//
//                System.out.println("Difference in seconds: " + difference);
//            } else {
//                System.out.println("ERROR PARSING");
//            }

//            System.out.println(XmlInputATMlog);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//            dbFactory.setCoalescing(true);
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            InputSource inputSource = new InputSource(new StringReader(XmlInputATMlog));
            Document doc = dBuilder.parse(inputSource);
            doc.getDocumentElement().normalize();
            NodeList operations = doc.getElementsByTagName("operation");


            Node operTag = operations.item(0);
            System.out.println(operTag);
            DocumentBuilderFactory targetFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder targetBuilder = targetFactory.newDocumentBuilder();
            Document targetDoc = targetBuilder.newDocument();
            Element operationElement = (Element) doc.getElementsByTagName("operation").item(0);
            System.out.println(operationElement);
            String currentTs = operationElement.getAttribute("current_ts").replace("T", " ");
            String ts = operationElement.getAttribute("ts");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            LocalDateTime currentTsDateTime = LocalDateTime.parse(currentTs, formatter);
            LocalDateTime tsDateTime = LocalDateTime.parse(ts, formatter);

            double currentTsSeconds = currentTsDateTime.getSecond() + currentTsDateTime.getNano() / 1e9;
            double tsSeconds = tsDateTime.getSecond() + tsDateTime.getNano() / 1e9;

            double result = currentTsSeconds - tsSeconds;
            System.out.println("result : " + result);

            System.out.println(ts);
            System.out.println(currentTs);
            String tableName = "ATM_LOG";
            Element insertRowElement = targetDoc.createElement("operation");

            System.out.println("TS : " + ts);
            String rowOpAttr = intenSeq();
//            System.out.println(bigint);
            insertRowElement.setAttribute("intentSEQ", rowOpAttr);
            double kafkprocess = result;
            operationElement.setAttribute("processKafka", String.valueOf(kafkprocess));

            copyAttributeNode2Node(operTag, insertRowElement);
            targetDoc.appendChild(insertRowElement);
            NodeList colElements = doc.getElementsByTagName("col");
            InputStream in = new FileInputStream("D:\\JAVA\\convertFormatXML\\src\\main\\java\\table_ATMLOG.yaml");
            Yaml yaml = new Yaml();

            Map<String, Object> data = yaml.load(in);

//            if (data == null) {
//
//                InputStream in = new FileInputStream("D:\\JAVA\\convertFormatXML\\src\\main\\java\\table_ATMLOG.yaml");
//                Yaml yaml = new Yaml();
//                data = yaml.load(in);
//                in.close(); // Close the stream after reading
//            }


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
//                                targetColElement.setAttribute("iskey",primaryKeyElement);
//                                Element beforeElement = targetDoc.createElement("before");
//                                beforeElement.setAttribute("missing", "true");
//                                targetColElement.appendChild(beforeElement);

                                String primaryKey = (String) column.get("primary_key");
                                String dataType = (String) column.get("data_type");

                                // Tạo thẻ <primary_key> và <data_type> cho cột hiện tại
                                Element dataTypeElementAFTER = targetDoc.createElement(dataType); // Tạo phần tử với tên là dataType
                                dataTypeElementAFTER.setTextContent(colValue);
                                targetColElement.appendChild(dataTypeElementAFTER);
                                targetColElement.setAttribute("PK",primaryKey);
                                 // Create and append <primary_key> and <data_type> elements
//                                Element primaryKeyElement = targetDoc.createElement("primary_key");
//                                primaryKeyElement.appendChild(targetDoc.createTextNode(primaryKey));
//                                targetColElement.appendChild(primaryKeyElement);
//                                Element dataTypeElement = targetDoc.createElement("data_type");
//                                dataTypeElement.appendChild(targetDoc.createTextNode(dataType));
//                                targetColElement.appendChild(dataTypeElement);
                                // Append the <col> element to the appropriate parent
                                insertRowElement.appendChild(targetColElement);
                            }
                        }
                    }
                }
            }
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            StringWriter stringWriter = new StringWriter();
            transformer.transform(new DOMSource(targetDoc), new StreamResult(stringWriter));
            String finalXMl = stringWriter.toString();
            System.out.println(finalXMl);
            long endTime = System.nanoTime();
            double executionTimeMs = (endTime - startTime) / 1e6;

            System.out.println("kafka: " + executionTimeMs + " m/s");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}