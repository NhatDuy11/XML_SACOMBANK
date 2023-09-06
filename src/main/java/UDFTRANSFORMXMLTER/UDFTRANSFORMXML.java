package UDFTRANSFORMXMLTER;

import org.w3c.dom.*;
import org.xml.sax.InputSource;

import javax.xml.parsers.*;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "xmlTransformer", description = "Transforms XML data")

public class UDFTRANSFORMXML {
    @Udf(description = "Transforms XML data to target format")
    public String transformXml(String xmlInput) {
        try {

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
            msgElement.setAttribute("dbName", "SWITCH");
            targetDoc.appendChild(msgElement);

            Element rowOpElement = targetDoc.createElement("rowOp");
            rowOpElement.setAttribute("authID", "SW_APP");
            rowOpElement.setAttribute("cmitLSN", "0000:0001:ea39:c7e0:0000:000e:c96c:4d20");
            rowOpElement.setAttribute("cmitTime", "2023-08-02T14:47:04.000005");
            msgElement.appendChild(rowOpElement);

            Element insertRowElement = targetDoc.createElement("insertRow");
            insertRowElement.setAttribute("subName", "ATMSTATUS0001");
            insertRowElement.setAttribute("srcOwner", "SW_OWN");
            insertRowElement.setAttribute("srcName", "ATMSTATUS");
            insertRowElement.setAttribute("hasLOBCols", "0");
            insertRowElement.setAttribute("intentSEQ", "0000:0001:ea39:c7df:0000:000e:c96c:4d16");
            rowOpElement.appendChild(insertRowElement);

            NodeList colElements = doc.getElementsByTagName("col");
            for (int i = 0; i < colElements.getLength(); i++) {
                Element colElement = (Element) colElements.item(i);
                String colName = colElement.getAttribute("name");
                String colValue = colElement.getElementsByTagName("after").item(0).getTextContent();

                Element targetColElement = targetDoc.createElement("col");
                targetColElement.setAttribute("name", colName);
                if (colName.equals("STATUS_DATE")) {
                    Element dateElement = targetDoc.createElement("date");
                    dateElement.appendChild(targetDoc.createTextNode(colValue.split(" ")[0]));
                    targetColElement.appendChild(dateElement);
                } else if (isInteger(colValue)) {
                    // Xử lý cột kiểu integer
                    Element intElement = targetDoc.createElement("integer");
                    intElement.appendChild(targetDoc.createTextNode(colValue));
                    targetColElement.appendChild(intElement);
                } else if (colValue.matches("^-?\\d+$")) {
                    Element smallIntElement = targetDoc.createElement("smallint");
                    smallIntElement.appendChild(targetDoc.createTextNode(colValue));
                    targetColElement.appendChild(smallIntElement);
                }
                else {
                    targetColElement.appendChild(targetDoc.createElement("varchar")).setTextContent(colValue);
                }
                insertRowElement.appendChild(targetColElement);
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
            return null;
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
}
