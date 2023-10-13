package UDFTRANSFORM;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.yaml.snakeyaml.Yaml;
import sun.jvm.hotspot.runtime.Bytes;

import javax.xml.parsers.*;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@UdfDescription(name = "ConvertXmlUdf_ATMLOG_byte_03", description = "Converts XML to target format")
public class ConvertXmlUdfByte {

    public byte[] convertByte2byte(Byte[] bigByte) {
        byte[] bytes = new byte[bigByte.length];

        int j = 0;
        // Unboxing Byte values. (Byte[] to byte[])
        for (Byte b : bigByte)
            bytes[j++] = b.byteValue();

        return bytes;
    }

    @Udf(description = "Convert XML")
    public String convertXml(@UdfParameter(value = "xmlInput") ByteBuffer xmlInput) throws Exception {
        try {
//            String xmlinput_str = xmlInput.replace("\u001C","");

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setCoalescing(true);
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//            InputSource inputSource = new InputSource(new StringReader(xmlinput_str));
            InputSource inputSource = new InputSource(new ByteArrayInputStream(xmlInput.array()));
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
            insertRowElement.setAttribute("srcOwner", "SW_OWN");
            insertRowElement.setAttribute("srcName", "ATMLOG");
            insertRowElement.setAttribute("hasLOBCols", "0");
            rowOpElement.appendChild(insertRowElement);

            NodeList colElements = doc.getElementsByTagName("col");
//            InputStream in = new FileInputStream("/tmp/ext/table_ATMLOG.yaml");
            InputStream in = new FileInputStream("/home/nhatduy/table_ATMLOG.yaml");
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(in);
            List<Map<String, Object>> tables = (List<Map<String, Object>>) ((List<Map<String, Object>>) data.get("database")).get(0).get("table");


            for (int i = 0; i < colElements.getLength(); i++) {
                Element colElement = (Element) colElements.item(i);
                String colName = colElement.getAttribute("name");
                String colValue = colElement.getElementsByTagName("after").item(0).getTextContent();

                Element targetColElement = targetDoc.createElement("col");
                targetColElement.setAttribute("name", colName);
                targetColElement.appendChild(targetDoc.createElement("after")).setTextContent(colValue);

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
