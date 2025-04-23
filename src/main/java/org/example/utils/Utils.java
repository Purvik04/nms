package org.example.utils;

import io.vertx.ext.web.RoutingContext;

public class Utils {

    private static final String REGEX_IPV4 = "^((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)(\\.|$)){4}$";

    public static String getTableNameFromContext(RoutingContext context)
    {
        String path  = context.normalizedPath().split("/")[1];

        return switch (path)
        {
            case "credentials" -> "credential_profiles";

            case "discovery" -> "discovery_profiles";

            case "provision" -> "provisioning_jobs";

            default -> throw new IllegalArgumentException("Unknown path: " + path);
        };
    }

    public static boolean isValidIPv4(String ip)
    {
        return ip != null && ip.matches(REGEX_IPV4);
    }

    public static boolean runPing(String ip) {
        try {
            var process = new ProcessBuilder("ping", "-c", "1", ip).start();
            return process.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean runPortCheck(String ip, int port) {
        try (var socket = new java.net.Socket()) {
            socket.connect(new java.net.InetSocketAddress(ip, port), 2000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
