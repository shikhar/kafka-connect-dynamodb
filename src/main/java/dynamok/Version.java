package dynamok;

public class Version {

    // TODO pull this in from a packaged resource controlled by the build
    private static final String VERSION = "0.1.0-SNAPSHOT";

    public static String get() {
        return VERSION;
    }

}
