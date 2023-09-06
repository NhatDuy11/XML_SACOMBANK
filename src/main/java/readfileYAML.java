import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

public class readfileYAML {
        public void ReadFile(){
                InputStream inputStream = this.getClass()
                        .getClassLoader()
                        .getResourceAsStream("table_ATMLOG.yaml");
                Yaml yaml = new Yaml();
                Map<String, Object>  object = yaml.load(inputStream);
                System.out.println(object);
        }





}
