import io.github.cdimascio.dotenv.Dotenv;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

class Config {
    //Required
    public static int PORT;
    public static String KAFKA_BROKER;
    public static String TOPIC;

    //Optional
    public static boolean BLOCKING;
    public static String READINESS_TOPIC;
    public static int LINGER_TIME_MS;
    public static String COMPRESSION_TYPE;

    //Autentication
    public static boolean AUTHENTICATED_KAFKA = false;
    public static String SECURITY_PROTOCOL;
    public static String TRUSTSTORE_LOCATION;
    public static String KEYSTORE_LOCATION;
    public static String TRUSTSTORE_PASSWORD;
    public static String KEYSTORE_PASSWORD;
    public static String KEY_PASSWORD;
    public static String SASL_USERNAME;
    public static String SASL_PASSWORD;

    //Monitoring
    public static boolean USE_PROMETHEUS;
    public static String PROMETHEUS_BUCKETS;

    public static void init() throws Exception {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        PORT = getInt(dotenv, "PORT");
        KAFKA_BROKER = getString(dotenv, "KAFKA_BROKER");

        TOPIC = getOptionalString(dotenv, "TOPIC", null);
        if (TOPIC != null) {
            throw new Error(
                "TOPIC as environment variable is not supported anymore, please pass topic in the producer request"
            );
        }

        READINESS_TOPIC = getOptionalString(dotenv, "READINESS_TOPIC", null);
        LINGER_TIME_MS = getOptionalInt(dotenv, "LINGER_TIME_MS", 0);
        COMPRESSION_TYPE = getOptionalString(dotenv, "COMPRESSION_TYPE", "none");
        BLOCKING = getOptionalBool(dotenv, "BLOCKING", false);

        String truststoreFilePath = getOptionalString(dotenv, "TRUSTSTORE_FILE_PATH", null);
        if (truststoreFilePath != null) {
            TRUSTSTORE_LOCATION = "client.truststore.jks";
            writeToFile(TRUSTSTORE_LOCATION, readFile(getString(dotenv, "TRUSTSTORE_FILE_PATH")));
            TRUSTSTORE_PASSWORD = readFile(getString(dotenv, "TRUSTSTORE_PASSWORD_FILE_PATH"));
        }

        SECURITY_PROTOCOL = getOptionalString(dotenv, "SECURITY_PROTOCOL", "");

        if (SECURITY_PROTOCOL.equals("SSL")) {
            KEYSTORE_LOCATION = "client.keystore.p12";
            KEYSTORE_PASSWORD = readFile(getString(dotenv, "KEYSTORE_PASSWORD_FILE_PATH"));
            writeToFile(KEYSTORE_LOCATION, readFile(getString(dotenv, "KEYSTORE_FILE_PATH")));
            KEY_PASSWORD = readFile(getString(dotenv, "KEY_PASSWORD_FILE_PATH"));
            AUTHENTICATED_KAFKA = true;
        }

        if (SECURITY_PROTOCOL.equals("SASL_SSL")) {
            SASL_USERNAME = getString(dotenv, "SASL_USERNAME");
            SASL_PASSWORD = readFile(getString(dotenv, "SASL_PASSWORD_FILE_PATH"));
            AUTHENTICATED_KAFKA = true;
        }

        USE_PROMETHEUS = getOptionalBool(dotenv, "USE_PROMETHEUS", false);
        PROMETHEUS_BUCKETS = getOptionalString(dotenv, PROMETHEUS_BUCKETS, "0.003,0.03,0.1,0.3,1.5,10");
    }

    private static void writeToFile(String path, String value) throws IOException {
        Files.write(Paths.get(path), Base64.getDecoder().decode(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static String readFile(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)));
    }

    private static String getString(Dotenv dotenv, String name) throws Exception {
        String value = dotenv.get(name);

        if (value == null) {
            throw new Exception("missing env var: " + name);
        }

        return value;
    }

    private static String getOptionalString(Dotenv dotenv, String name, String fallback) {
        try {
            return getString(dotenv, name);
        } catch (Exception e) {
            return fallback;
        }
    }

    private static int getInt(Dotenv dotenv, String name) {
        return Integer.parseInt(dotenv.get(name));
    }

    private static int getOptionalInt(Dotenv dotenv, String name, int fallback) {
        try {
            return Integer.parseInt(dotenv.get(name));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static boolean getOptionalBool(Dotenv dotenv, String name, boolean fallback) {
        try {
            return Boolean.parseBoolean(getString(dotenv, name));
        } catch (Exception e) {
            return fallback;
        }
    }
}
