package UDFTRANSFORM;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
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
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;


@UdfDescription(name = "ConvertXmlUdf_ATMLOG_stable_10", description = "Converts XML to target format")
public class ConvertXmlUdf_Stable {
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

            Element msgElement = targetDoc.createElement("msg");
            msgElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            msgElement.setAttribute("xsi:noNamespaceSchemaLocation", "mqcap.xsd");
            targetDoc.appendChild(msgElement);

            Element rowOpElement = targetDoc.createElement("rowOp");
            String rowOpAttr = intenSeq();

            rowOpElement.setAttribute("intentSEQ",rowOpAttr);

            msgElement.appendChild(rowOpElement);

            Element insertRowElement = targetDoc.createElement("Row");
            copyAttributeNode2Node(operTag,insertRowElement);
          //  insertRowElement.setAttribute("srcOwner", "SW_OWN");
          //  insertRowElement.setAttribute("srcName", tableName);
          //  insertRowElement.setAttribute("hasLOBCols", "0");
            rowOpElement.appendChild(insertRowElement);
            NodeList colElements = doc.getElementsByTagName("col");
            InputStream in = new FileInputStream("/tmp/ext/table_ATMLOG.yaml");
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

//                  System.out.println(tableMetadata.get("columns"));
                    String metaName = (String) tableMetadata.get("name");
                    if (tableName.equals(metaName)) {
//                        System.out.println(String.format("Processing table %s", tableName));
                        List<Map<String, Object>> columns = (List<Map<String, Object>>) tableMetadata.get("columns");
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
            return convertedXml;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
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
