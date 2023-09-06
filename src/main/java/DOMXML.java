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
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static javax.swing.UIManager.get;

public class DOMXML {
    public static void main(String[] args)throws FileNotFoundException {
        try{
            String InputXML = ReadFile.readFile("test_xml.xml", Charset.defaultCharset()).replace("\u001C", "");
            //System.out.println(InputXML);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            InputSource inputSource = new InputSource(new StringReader(InputXML));
            //System.out.println(inputSource);
            DocumentBuilder dbuilder = factory.newDocumentBuilder();
            Document doc = dbuilder.parse(inputSource);
            System.out.println(doc);
            DocumentBuilderFactory targerFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder targerBuilder = targerFactory.newDocumentBuilder();
            Document targetDoc = targerBuilder.newDocument();
            System.out.println(targetDoc);
            NodeList colElemnts = doc.getElementsByTagName("col");
            System.out.println(colElemnts);

            InputStream in = new FileInputStream("D:\\JAVA\\convertFormatXML\\status.yaml");
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(in);
            List <Map<String,Object>> tables = (List<Map<String,Object>>) data.get("database");
            String tableName = "ATM_LOG";
            for ( int i = 0 ; i<colElemnts.getLength();i++){
                Element colelement = (Element) colElemnts.item(i);
                String colname = colelement.getAttribute("name");
                Element targetColElement = targetDoc.createElement("col");

//                System.out.println(colname);
                for(Map<String,Object> table:tables){
                    Map<String,Object> Mapping = table;
                    Map<String,Object> tableMetadata = (Map<String, Object>) Mapping.get("table");
                    String metaName = (String) tableMetadata.get("name");
                    System.out.println(metaName);
                        List<Map<String,Object>> columns = (List<Map<String, Object>>) tableMetadata.get("columns");
                     for(Map<String,Object> column:columns){
                         String Columnname = (String) column.get("name");
                         if(Columnname.equals(colname)){
                             String primary_key = (String) column.get("primary_key");
                             String dataType = (String) column.get("data)type");
//                             System.out.println(primary_key);
                             Element primarykey_element = targetDoc.createElement("primary_key");
                             primarykey_element.appendChild(targetDoc.createTextNode(primary_key));
                             targetColElement.appendChild(primarykey_element);


                         }
                     }

                }



//                System.out.println(colelement);
            }
            //System.out.println(data);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            String convertedXml = writer.getBuffer().toString();
            System.out.println(convertedXml);
        }

        catch (Exception e){
            //int i = Integer.parseInt("D:\\JAVA\\convertFormatXML\\test_xml");
            e.printStackTrace();
        }
    }
//    private static Map<String, Object> getColumnDataFromYaml(Map<String, Object> yamlData, String colName) {
//        List<Map<String, Object>> columns = (List<Map<String, Object>>) yamlData.get("columns");
//
//        if (columns != null) {
//            for (Map<String, Object> column : columns) {
//                if (colName.equals(column.get("columns"))) {
//                    return column;
//                }
//            }
//        }
//
//
//        return null;
//    }
}