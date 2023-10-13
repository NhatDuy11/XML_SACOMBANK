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
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;


public class test2_xml {

    public static void copyAttributeNode2Node(Node operationTag, Element insertRowElement) {

        for (int i = 0; i < operationTag.getAttributes().getLength(); i++) {
            String name = operationTag.getAttributes().item(i).getNodeName();
            String value="";
            if (name=="current_ts") {
                value = operationTag.getAttributes().item(i).getNodeValue().replace("T", " ");
            }else {
                value = operationTag.getAttributes().item(i).getNodeValue();
            }

            insertRowElement.setAttribute(name, value);
        }

    }

    public static String intenSeq() {
        String lUUID = String.format("%040d", new BigInteger(UUID.randomUUID().toString().replace("-", ""), 16));
        return lUUID;
    }

    public static void main(String[] args) {
        try {
            long startTime = System.nanoTime();
            //String xmlInputATMLog = "<?xml version='1.0' encoding='UTF-8'?>\n<operation table='SACOM_SW_OWN.ATM_LOG' type='I' ts='2023-08-22 13:49:52.364222' current_ts='2023-08-22T13:50:00.210000' pos='00000000020001362579' numCols='8'>\n <col name='SHCLOG_ID' index='0'>\n  <before missing='true'/>\n <after><![CDATA[AAEAsgAkZN52WQAB]]></after>\n </col>\n <col name='INSTITUTION_ID' index='1'>\n  <before missing='true'/>\n <after><![CDATA[1]]></after>\n </col>\n <col name='GROUP_NAME' index='2'>\n<before missing='true'/>\n<after><![CDATA[CDM5020105]]></after>\n </col>\n <col name='UNIT' index='3'>\n <before missing='true'/>\n  <after><![CDATA[22]]></after>\n </col>\n <col name='FUNCTION_CODE' index='4'>\n <before missing='true'/>\n<after><![CDATA[200]]></after>\n </col>\n <col name='LOGGED_TIME' index='5'>\n  <before missing='true'/>\n <after><![CDATA[2023-08-18 02:34:49.000000000]]></after>\n </col>\n <col name='LOG_DATA' index='6'>\n  <before missing='true'/>\n  <after><![CDATA[22\u001c022\u001c\u001c9]]></after>\n </col>\n <col name='SITE_ID' index='7'>\n  <before missing='true'/>\n<after><![CDATA[1]]></after>\n </col>\n</operation>\n";
            String XmlInputATMlog = ReadFile.readFile("ATM_LOG.txt", StandardCharsets.UTF_8).replace("\u001C", "");
//            System.out.println(XmlInputATMlog);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//            dbFactory.setCoalescing(true);
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            InputSource inputSource = new InputSource(new StringReader(XmlInputATMlog));
            Document doc = dBuilder.parse(inputSource);
            doc.getDocumentElement().normalize();

            NodeList operations = doc.getElementsByTagName("operation");
            Node operTag = operations.item(0);




            DocumentBuilderFactory targetFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder targetBuilder = targetFactory.newDocumentBuilder();
            Document targetDoc = targetBuilder.newDocument();

//            operation.

            Element msgElement = targetDoc.createElement("msg");
            msgElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            msgElement.setAttribute("xsi:noNamespaceSchemaLocation", "mqcap.xsd");

            targetDoc.appendChild(msgElement);


            Element rowOpElement = targetDoc.createElement("rowOp");

            String rowOpAttr = intenSeq();

            rowOpElement.setAttribute("intentSEQ",rowOpAttr);
            //String opeAttribute = operTag.getAttributes().getNamedItem("table").getNodeValue();

                    msgElement.appendChild(rowOpElement);


            String tableName = "ATM_LOG";
            Element insertRowElement = targetDoc.createElement("Row");

            copyAttributeNode2Node(operTag,insertRowElement);


            //insertRowElement.setAttribute("srcOwner", "SW_OWN");
            //insertRowElement.setAttribute("srcName", tableName);
            rowOpElement.appendChild(insertRowElement);

            NodeList colElements = doc.getElementsByTagName("col");
            InputStream in = new FileInputStream("D:\\JAVA\\convertFormatXML\\src\\main\\java\\table_ATMLOG.yaml");
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(in);
            List<Map<String, Object>> tables = (List<Map<String, Object>>) ((List<Map<String, Object>>) data.get("database"));
            for (int i = 0; i < colElements.getLength(); i++) {
                Element colElement = (Element) colElements.item(i);
                String colName = colElement.getAttribute("name");
                String colValue = colElement.getElementsByTagName("after").item(0).getTextContent();

                Element targetColElement = targetDoc.createElement("col");
                targetColElement.setAttribute("index",Integer.toString(i));
                targetColElement.setAttribute("name", colName);
                targetColElement.appendChild(targetDoc.createElement("after")).setTextContent(colValue);

                insertRowElement.appendChild(targetColElement);

                for (Map<String, Object> table : tables) {
                    Map<String, Object> tmpMapping = (Map<String, Object>) table;
                    Map<String, Object> tableMetadata = (Map<String, Object>) tmpMapping.get("table");

//                        System.out.println(tableMetadata.get("columns"));
                    String metaName = (String) tableMetadata.get("name");

                    if (tableName.equals(metaName)) {
//                        System.out.println(String.format("Processing table %s", tableName));
                        List<Map<String, Object>> columns        = (List<Map<String, Object>>) tableMetadata.get("columns");
                        for (Map<String, Object> column : columns) {
                            String columnName = (String) column.get("name");
                            if (columnName.equals(colName)) {
                                String primaryKey = (String) column.get("primary_key");
                                String dataType = (String) column.get("data_type");

                                // Tạo thẻ <primary_key> và <data_type> cho cột hiện tại
                                Element primaryKeyElement = targetDoc.createElement("primary_key");
                                primaryKeyElement.appendChild(targetDoc.createTextNode(primaryKey));
                                targetColElement.appendChild(primaryKeyElement);

                                Element dataTypeElement = targetDoc.createElement("data_type");
                                dataTypeElement.appendChild(targetDoc.createTextNode(dataType));
                                targetColElement.appendChild(dataTypeElement);
                            }
                        }
                    }
                }
                rowOpElement.appendChild(insertRowElement);
            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(targetDoc), new StreamResult(writer));
            String convertedXml = writer.getBuffer().toString();

            System.out.println(convertedXml);
            long endTime = System.nanoTime();
           double executionTimeMs = (endTime - startTime) / 1e6;
//
            System.out.println("Process Kafka:  " + executionTimeMs + " ms");




        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isSmallInt(String str) {
        try {
            Short.parseShort(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }

    }

//    private static void addXmlElement(Document document, Element parentElement, String tagName, String textContent) {
//        Element element = document.createElement(tagName);
//        element.appendChild(document.createTextNode(textContent));
//        parentElement.appendChild(element);
//    }
//
//    private static boolean isDate(String str) {
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//        dateFormat.setLenient(false);
//        try {
//            dateFormat.parse(str);
//            return true;
//        } catch (ParseException e) {
//            return false;
//        }
//    }


    private static boolean isDecimal(String str) {
        try {
            new BigDecimal(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}