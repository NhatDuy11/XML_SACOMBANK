import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
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
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class test_4 {
    public static void main(String[] args) {
        try {


            String xmlInput = "<?xml version='1.0' encoding='UTF-8'?>\n" +
                    "<operation table='SACOM_SW_OWN.ATMSTATUS' type='I' ts='2023-08-22 13:56:19.361910' current_ts='2023-08-22T13:56:26.563000' pos='00000000020001370425' numCols='10'>\n" +
                    " <col name='UNIT' index='0'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[88]]></after>\n" +
                    " </col>\n" +
                    " <col name='GROUP_NAME' index='1'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[CDM5020105]]></after>\n" +
                    " </col>\n" +
                    " <col name='STATUS_DATE' index='2'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[2023-08-07 00:00:00]]></after>\n" +
                    " </col>\n" +
                    " <col name='STATUS_TIME' index='3'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[23010]]></after>\n" +
                    " </col>\n" +
                    " <col name='STATUS' index='4'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[619]]></after>\n" +
                    " </col>\n" +
                    " <col name='SDESC' index='5'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[DISPENSER NO NOTES DISPENSED            ]]></after>\n" +
                    " </col>\n" +
                    " <col name='O_ROWID' index='6'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[0]]></after>\n" +
                    " </col>\n" +
                    " <col name='INSTITUTIONID' index='7'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[1]]></after>\n" +
                    " </col>\n" +
                    " <col name='SITE_ID' index='8'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[1]]></after>\n" +
                    " </col>\n" +
                    " <col name='LOG_ID' index='9'>\n" +
                    "   <before missing='true'/>\n" +
                    "   <after><![CDATA[AAEBaADMZM/0wgAB]]></after>\n" +
                    " </col>\n" +
                    "</operation>";


            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            InputSource inputSource = new InputSource(new StringReader(xmlInput));
            Document doc = dBuilder.parse(inputSource);
            doc.getDocumentElement().normalize();

            DocumentBuilderFactory targetFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder targetBuilder = targetFactory.newDocumentBuilder();
            Document targetDoc = targetBuilder.newDocument();

            Element msgElement = targetDoc.createElement("msg");
            msgElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            msgElement.setAttribute("xsi:noNamespaceSchemaLocation", "mqcap.xsd");
            msgElement.setAttribute("version", "1.0.0");
            msgElement.setAttribute("dbName", "ISTP");
            targetDoc.appendChild(msgElement);

            Element rowOpElement = targetDoc.createElement("rowOp");

            msgElement.appendChild(rowOpElement);

            Element insertRowElement = targetDoc.createElement("insertRow");
            insertRowElement.setAttribute("subName", "ATMSTATUS0001");
            insertRowElement.setAttribute("srcOwner", "SW_OWN");
            insertRowElement.setAttribute("srcName", "ATMLOG");
            insertRowElement.setAttribute("hasLOBCols", "0");
            rowOpElement.appendChild(insertRowElement);

            NodeList colElements = doc.getElementsByTagName("col");
            InputStream in = new FileInputStream("\\tmp\\ext\\table_ATMLOG.yaml");
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(in);
            List<Map<String, Object>> tables = (List<Map<String, Object>>) ((List<Map<String, Object>>) data.get("database")).get(0).get("table");


            for (int i = 0; i < colElements.getLength(); i++) {
                Element colElement = (Element) colElements.item(i);
                String colName = colElement.getAttribute("name");
                String colValue = colElement.getElementsByTagName("after").item(0).getTextContent();

                Element targetColElement = targetDoc.createElement("col");
                targetColElement.setAttribute("name", colName);
                if (isDate(colValue.split(" ")[0])) {
                    addXmlElement(targetDoc, targetColElement, "date", colValue.split(" ")[0]);

                } else if (isInteger(colValue)) {
                    // Xử lý cột kiểu integer
                    Element intElement = targetDoc.createElement("intege");
                    intElement.appendChild(targetDoc.createTextNode(colValue));
                    targetColElement.appendChild(intElement);
                } else if (colValue.matches("^-?\\d+$")) {
                    // Xử lý cột kiểu smallint
                    Element smallIntElement = targetDoc.createElement("smallint");
                    smallIntElement.appendChild(targetDoc.createTextNode(colValue));
                    targetColElement.appendChild(smallIntElement);
                }
                else if (colValue.equalsIgnoreCase("bitvarchar")) {
                    // Xử lý cột kiểu bitvarchar
                    Element bitVarcharElement = targetDoc.createElement("bitvarchar");
                    bitVarcharElement.appendChild(targetDoc.createTextNode(colValue));
                    targetColElement.appendChild(bitVarcharElement);
                }
                else if (isDecimal(colValue)) {
                    // Xử lý cột kiểu decimal
                    Element decimalElement = targetDoc.createElement("decimal");
                    decimalElement.appendChild(targetDoc.createTextNode(colValue));
                    targetColElement.appendChild(decimalElement);
                }

                else {
                    // Xử lý các cột khác (varchar)
                    targetColElement.appendChild(targetDoc.createElement("varchar")).setTextContent(colValue);
                }
                insertRowElement.appendChild(targetColElement);

                for (Map<String, Object> table : tables) {
                    List<Map<String, Object>> columns = (List<Map<String, Object>>) table.get("columns");
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
                rowOpElement.appendChild(insertRowElement);


            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(targetDoc), new StreamResult(writer));
            String convertedXml = writer.getBuffer().toString();

            System.out.println(convertedXml);
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
    private static void addXmlElement(Document document, Element parentElement, String tagName, String textContent) {
        Element element = document.createElement(tagName);
        element.appendChild(document.createTextNode(textContent));
        parentElement.appendChild(element);
    }

    private static boolean isDate(String str) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setLenient(false);
        try {
            dateFormat.parse(str);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }


    private static boolean isDecimal(String str) {
        try {
            new BigDecimal(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }

    }
}
