import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ATMLOG {

    private String database;
    private List<table_ATMLOG> table;

    public static class table_ATMLOG {
        private String name;
        private List<Column> columns;

        public static class Column {
            private String name;
            private String primary_key;
            private String data_type;
        }
    }
}
