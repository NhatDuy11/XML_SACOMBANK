
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

public class test1_xml {
    public static void main(String[] args) {
        try {
            // XML input string
            String inputXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><msg xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation=\"mqcap.xsd\" version=\"1.0.0\" dbName=\"SWITCH\"><rowOp authID=\"SW_APP  \" cmitLSN=\"0000:0001:ea39:c7e0:0000:000e:c96c:4d20\" cmitTime=\"2023-08-02T14:47:04.000005\"><insertRow subName=\"ATMSTATUS0001\"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   srcOwner=\"SW_OWN\" srcName=\"ATMSTATUS\" hasLOBCols=\"0\" intentSEQ=\"0000:0001:ea39:c7df:0000:000e:c96c:4d16\"><col name=\"LOG_ID\" isKey=\"1\"><varchar>AAEBkgCWZMpsaAAA</varchar></col><col name=\"SITE_ID\" isKey=\"1\"><smallint>1</smallint></col><col name=\"GROUP_NAME\"><varchar>BAC5050101</varchar></col><col name=\"INSTITUTIONID\"><varchar>1</varchar></col><col name=\"O_ROWID\"><integer>0</integer></col><col name=\"SDESC\"><char>1st CASSETTE IS LOW                     </char></col><col name=\"STATUS\"><smallint>760</smallint></col><col name=\"STATUS_DATE\"><date>2023-08-02</date></col><col name=\"STATUS_TIME\"><integer>214704</integer></col><col name=\"UNIT\"><smallint>45</smallint></col></insertRow></rowOp></msg>";

            // Parse the input XML
            Document inputDoc = DocumentHelper.parseText(inputXml);

            // Create a new XML document for the output format
            Document outputDoc = DocumentHelper.createDocument();
            Element root = outputDoc.addElement("msg");
            root.addAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            root.addAttribute("xsi:noNamespaceSchemaLocation", "mqcap.xsd");
            root.addAttribute("version", "1.0.0");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");
            root.addAttribute("dbName", "SWITCH");



            // ... build the output XML structure ...

            // Create an XMLWriter to write the output
            OutputFormat format = OutputFormat.createPrettyPrint();
            XMLWriter writer = new XMLWriter(System.out, format);

            // Write the output XML
            writer.write(outputDoc);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
