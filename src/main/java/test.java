import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
public class test {

    static readfileYAML read = new readfileYAML();
    public static void main(String[] args) {
        ReadFile();
    }
    private static void ReadFile(){
        read.ReadFile();
    }
}
