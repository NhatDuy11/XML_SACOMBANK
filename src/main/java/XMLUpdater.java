import org.w3c.dom.*;
import javax.xml.parsers.*;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;

import org.xml.sax.InputSource;
import org.yaml.snakeyaml.*;
import java.util.*;

public class XMLUpdater {
    public static void main(String[] args) {
        try {
            String xmlInput = "<?xml version='1.0' encoding='UTF-8'?>\n<operation table='SACOM_SW_OWN.ATM_LOG' type='I' ts='2023-08-22 13:49:52.364222' current_ts='2023-08-22T13:50:00.210000' pos='00000000020001362579' numCols='8'>\n <col name='SHCLOG_ID' index='0'>\n   <before missing='true'/>\n   <after><![CDATA[AAEAsgAkZN52WQAB    ]]></after>\n </col>\n <col name='INSTITUTION_ID' index='1'>\n   <before missing='true'/>\n   <after><![CDATA[1]]></after>\n </col>\n <col name='GROUP_NAME' index='2'>\n   <before missing='true'/>\n   <after><![CDATA[CDM5020105]]></after>\n </col>\n <col name='UNIT' index='3'>\n   <before missing='true'/>\n   <after><![CDATA[22]]></after>\n </col>\n <col name='FUNCTION_CODE' index='4'>\n   <before missing='true'/>\n   <after><![CDATA[200]]></after>\n </col>\n <col name='LOGGED_TIME' index='5'>\n   <before missing='true'/>\n   <after><![CDATA[2023-08-18 02:34:49.000000000]]></after>\n </col>\n <col name='LOG_DATA' index='6'>\n   <before missing='true'/>\n   <after><![CDATA[22\u001c022\u001c\u001c9]]></after>\n </col>\n <col name='SITE_ID' index='7'>\n   <before missing='true'/>\n   <after><![CDATA[1]]></after>\n </col>\n</operation>\n";
            xmlInput = xmlInput.replace("\u001c", "");
            FileInputStream yamlInput = new FileInputStream("D:\\JAVA\\convertFormatXML\\src\\main\\java\\table_ATMLOG.yaml");
            Yaml yaml = new Yaml();
            Map<String, Object> yamlData = yaml.load(yamlInput);
            List<Map<String, Object>> columns = (List<Map<String, Object>>) ((Map<String, Object>) ((List<Map<String, Object>>) yamlData.get("database")).get(0)).get("columns");

            if (columns != null) {
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                InputSource inputSource = new InputSource(new StringReader(xmlInput));
                Document doc = dBuilder.parse(inputSource);
                doc.getDocumentElement().normalize();

                NodeList colNodes = doc.getElementsByTagName("col");
                for (int i = 0; i < colNodes.getLength(); i++) {
                    Element colElement = (Element) colNodes.item(i);
                    String colName = colElement.getAttribute("name");
                    for (Map<String, Object> colInfo : columns) {
                        if (colInfo.get("name").equals(colName)) {
                            String primaryKey = colInfo.get("primary_key").toString();
                            String dataType = colInfo.get("data_type").toString();

                            Element primaryKeyElement = doc.createElement("primary_key");
                            primaryKeyElement.appendChild(doc.createTextNode(primaryKey));
                            colElement.appendChild(primaryKeyElement);

                            Element dataTypeElement = doc.createElement("data_type");
                            dataTypeElement.appendChild(doc.createTextNode(dataType));
                            colElement.appendChild(dataTypeElement);

                            break;
                        }
                    }
                }
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                StringWriter writer = new StringWriter();
                transformer.transform(new DOMSource(doc), new StreamResult(writer));
                String updatedXml = writer.toString();

                System.out.println(updatedXml);
            } else {
                System.out.println("Unable to retrieve columns from YAML data.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}