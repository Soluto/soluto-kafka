import io.github.cdimascio.dotenv.Dotenv;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import org.json.JSONException;
import org.json.JSONObject;

class Config {
    //Required
    public static String KAFKA_BROKER;
    public static String TOPIC;
    public static String GROUP_ID;
    public static String SENDING_PROTOCOL;
    public static String TARGET;

    //Optional
    public static String RETRY_TOPIC;
    public static String DEAD_LETTER_TOPIC;
    public static boolean DEDUP_PARTITION_BY_KEY;
    public static int CONCURRENCY;
    public static int CONCURRENCY_PER_PARTITION;
    public static int PROCESSING_DELAY;
    public static int RETRY_PROCESSING_DELAY;
    public static int POLL_RECORDS;
    public static int CONSUMER_POLL_TIMEOUT;
    public static int CONSUMER_THREADS;
    public static int MANAGEMENT_SERVER_PORT;
    public static boolean DEBUG;

    //Authentication
    public static boolean AUTHENTICATED_KAFKA = false;
    public static String SECURITY_PROTOCOL;
    public static String KAFKA_PASSWORD;
    public static String TRUSTSTORE_LOCATION;
    public static String KEYSTORE_LOCATION;
    public static String SASL_USERNAME;
    public static String SASL_PASSWORD;

    //Monitoring
    public static boolean STATSD_CONFIGURED = false;
    public static String STATSD_CONSUMER_NAME;
    public static String STATSD_API_KEY;
    public static String STATSD_ROOT;
    public static String STATSD_HOST;
    public static boolean USE_PROMETHEUS;
    public static boolean HIDE_CONSUMED_MESSAGE;

    public static void init() throws Exception {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        KAFKA_BROKER = getString(dotenv, "KAFKA_BROKER");
        TOPIC = getString(dotenv, "TOPIC");
        GROUP_ID = getString(dotenv, "GROUP_ID");

        SENDING_PROTOCOL = getString(dotenv, "SENDING_PROTOCOL");
        TARGET = getString(dotenv, "TARGET");

        RETRY_TOPIC = getOptionalString(dotenv, "RETRY_TOPIC", null);
        DEAD_LETTER_TOPIC = getOptionalString(dotenv, "DEAD_LETTER_TOPIC", null);
        CONCURRENCY = getOptionalInt(dotenv, "CONCURRENCY", 1);
        CONCURRENCY_PER_PARTITION = getOptionalInt(dotenv, "CONCURRENCY_PER_PARTITION", 1);
        DEDUP_PARTITION_BY_KEY = getOptionalBool(dotenv, "DEDUP_PARTITION_BY_KEY", false);
        PROCESSING_DELAY = getOptionalInt(dotenv, "PROCESSING_DELAY", 0);
        RETRY_PROCESSING_DELAY = getOptionalInt(dotenv, "RETRY_PROCESSING_DELAY", 60000);

        CONSUMER_POLL_TIMEOUT = getOptionalInt(dotenv, "CONSUMER_POLL_TIMEOUT", 100);
        CONSUMER_THREADS = getOptionalInt(dotenv, "CONSUMER_THREADS", 4);
        POLL_RECORDS = getOptionalInt(dotenv, "POLL_RECORDS", 50);
        MANAGEMENT_SERVER_PORT = getOptionalInt(dotenv, "MANAGEMENT_SERVER_PORT", 0);
        DEBUG = getOptionalBool(dotenv, "DEBUG", false);

        JSONObject secrets = buildSecrets(dotenv);

        SECURITY_PROTOCOL = getOptionalString(dotenv, "SECURITY_PROTOCOL", "");
        if (SECURITY_PROTOCOL.equals("SSL")) {
            KAFKA_PASSWORD = getSecret(secrets, dotenv, "KAFKA_PASSWORD");
            String truststore = getSecret(secrets, dotenv, "TRUSTSTORE");
            String keystore = getSecret(secrets, dotenv, "KEYSTORE");
            TRUSTSTORE_LOCATION = "client.truststore.jks";
            KEYSTORE_LOCATION = "client.keystore.p12";
            writeToFile(TRUSTSTORE_LOCATION, truststore);
            writeToFile(KEYSTORE_LOCATION, keystore);
            AUTHENTICATED_KAFKA = true;
        }

        if (SECURITY_PROTOCOL.equals("SASL_SSL")) {
            SASL_USERNAME = getString(dotenv, "SASL_USERNAME");
            SASL_PASSWORD = getSecret(secrets, dotenv, "SASL_PASSWORD");
            AUTHENTICATED_KAFKA = true;
        }

        STATSD_CONSUMER_NAME = getOptionalString(dotenv, "STATSD_CONSUMER_NAME", null);
        if (STATSD_CONSUMER_NAME != null) {
            STATSD_API_KEY = getSecret(secrets, dotenv, "STATSD_API_KEY");
            STATSD_ROOT = getString(dotenv, "STATSD_ROOT");
            STATSD_HOST = getString(dotenv, "STATSD_HOST");
            STATSD_CONFIGURED = true;
        }
        USE_PROMETHEUS = getOptionalBool(dotenv, "USE_PROMETHEUS", false);
        HIDE_CONSUMED_MESSAGE = getOptionalBool(dotenv, "HIDE_CONSUMED_MESSAGE", false);
    }

    private static void writeToFile(String path, String value) throws IOException {
        Files.write(Paths.get(path), Base64.getDecoder().decode(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static JSONObject buildSecrets(Dotenv dotenv) {
        return readSecrets(getOptionalString(dotenv, "SECRETS_FILE_LOCATION", null));
    }

    private static JSONObject readSecrets(String secretsFileLocation) {
        if (secretsFileLocation == null) {
            return new JSONObject();
        }

        try {
            return new JSONObject(new String(Files.readAllBytes(Paths.get(secretsFileLocation))));
        } catch (IOException e) {
            return new JSONObject();
        }
    }

    private static String getSecret(JSONObject secrets, Dotenv dotenv, String name) throws Exception {
        String value = dotenv.get(name);

        if (value != null) {
            return value;
        }

        String secret = null;

        try {
            secret = secrets.getString(name);
        } catch (JSONException ignored) {
            throw new Exception("missing env var: " + name);
        }

        return secret;
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

    private static boolean getOptionalBool(Dotenv dotenv, String name, boolean fallback) {
        try {
            return Boolean.parseBoolean(getString(dotenv, name));
        } catch (Exception e) {
            return fallback;
        }
    }

    private static int getOptionalInt(Dotenv dotenv, String name, int fallback) {
        try {
            return Integer.parseInt(dotenv.get(name));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
