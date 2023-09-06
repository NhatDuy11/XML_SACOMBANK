import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class XmlFormatConverter {
    public static void main(String[] args) throws TransformerException {
        try {

            String xmlInput = "<root><after><![CDATA[2023-08-07 00:00:00]]></after></root>"; // Thay đổi giá trị tùy theo nhu cầu
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            InputSource inputSource = new InputSource(new StringReader(xmlInput));
            Document doc = dBuilder.parse(inputSource);
            doc.getDocumentElement().normalize();

            // Lấy phần tử <after>
            Element afterElement = (Element) doc.getElementsByTagName("after").item(0);

            // Lấy nội dung trong CDATA
            String cdataContent = afterElement.getFirstChild().getNodeValue();

            // Tạo phần tử <date> mới với giá trị từ CDATA
            Element dateElement = doc.createElement("date");
            dateElement.appendChild(doc.createTextNode(cdataContent));

            // Thay thế phần tử <after> bằng <date>
            Node parentNode = afterElement.getParentNode();
            parentNode.replaceChild(dateElement, afterElement);

            // Chuyển đổi Document thành String XML
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            String modifiedXml = writer.getBuffer().toString();

            System.out.println(modifiedXml); // In ra XML đã được sửa đổi
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
