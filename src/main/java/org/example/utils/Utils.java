package org.example.utils;

import io.vertx.ext.web.RoutingContext;

public class Utils {

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
        if (ip == null || ip.isEmpty()) return false;

        var parts = ip.split("\\.");

        if (parts.length != 4) return false;

        for (var part : parts)
        {
            try
            {
                var num = Integer.parseInt(part);

                if (num < 0 || num > 255) return false;

                // Disallow leading zeros (e.g. "01", "001")
                if (part.length() > 1 && part.startsWith("0")) return false;

            }
            catch (Exception exception)
            {
                return false;
            }
        }

        return true;
    }
}
