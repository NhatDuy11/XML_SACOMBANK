import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
public class test_xml {
    public static void main(String[] args) {
        try {
            // Create a new XML document for the output format
            Document outputDoc = DocumentHelper.createDocument();
            Element root = outputDoc.addElement("msg");
            root.addAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            root.addAttribute("xsi:noNamespaceSchemaLocation", "mqcap.xsd");
            root.addAttribute("version", "1.0.0");
            root.addAttribute("dbName", "SWITCH");

            Element rowOp = root.addElement("rowOp");
            rowOp.addAttribute("authID", "SW_APP");
            rowOp.addAttribute("cmitLSN", "0000:0001:ea39:c7d9:0000:000e:c96c:4d02");
            rowOp.addAttribute("cmitTime", "2023-08-02T14:47:03.000021");

            Element insertRow = rowOp.addElement("insertRow");
            insertRow.addAttribute("subName", "ATM_LOG0001");
            insertRow.addAttribute("srcOwner", "SW_OWN");
            insertRow.addAttribute("srcName", "ATM_LOG");
            insertRow.addAttribute("hasLOBCols", "0");
            insertRow.addAttribute("intentSEQ", "0000:0001:ea39:c7d8:0000:000e:c96c:4cfd");

            Element colFunctionCode = insertRow.addElement("col");
            colFunctionCode.addAttribute("name", "FUNCTION_CODE");
            Element integerFunctionCode = colFunctionCode.addElement("integer");
            integerFunctionCode.setText("200");

            Element colVarcharCode = insertRow.addElement("col");
            colVarcharCode.addAttribute("name","GROUP_NAME");
            Element varcharFunctionCode = colVarcharCode.addElement("varchar");
            varcharFunctionCode.setText("TNB5050101");






            // ... add more elements for other columns ...

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
