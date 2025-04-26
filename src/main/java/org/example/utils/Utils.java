package org.example.utils;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Utils {

    private static final String REGEX_IPV4 = "^((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)(\\.|$)){4}$";

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static String buildPlaceholders(int count)
    {
        var sb = new StringBuilder();

        for (int i = 1; i <= count; i++)
        {
            sb.append("$").append(i);

            if (i < count) sb.append(", ");
        }

        return sb.toString();
    }

    public static String getTableNameFromContext(RoutingContext context)
    {
        String path  = context.normalizedPath().split("/")[1];

        return switch (path)
        {
            case "credentials" -> "credential_profiles";

            case "discovery" -> "discovery_profiles";

            case "provision" -> "provisioning_jobs";

            default -> "";
        };
    }

    public static boolean isValidIPv4(String ip)
    {
        return ip != null && ip.matches(REGEX_IPV4);
    }

    public static boolean validateRequest(JsonObject requestBody,RoutingContext context) {

        var tableName = getTableNameFromContext(context);

        return switch (tableName)
        {
            case "credential_profiles" -> validateCredentialProfiles(requestBody);

            case "discovery_profiles" -> validateDiscoveryProfiles(requestBody);

            default -> false;
        };
    }

    private static boolean validateCredentialProfiles(JsonObject requestBody)
    {
        return requestBody.containsKey("credential_profile_name")
                && requestBody.getValue("credential_profile_name") instanceof String
                && requestBody.containsKey("system_type")
                && requestBody.getValue("system_type") instanceof String
                && requestBody.containsKey("credentials")
                && requestBody.getValue("credentials") instanceof JsonObject;
    }

    private static boolean validateDiscoveryProfiles(JsonObject requestBody)
    {
        return requestBody.containsKey("discovery_profile_name")
                && requestBody.getValue("discovery_profile_name") instanceof String
                && requestBody.containsKey("credential_profile_id")
                && requestBody.getValue("credential_profile_id") instanceof Integer
                && requestBody.containsKey("ip")
                && requestBody.getValue("ip") instanceof String
                && isValidIPv4(requestBody.getValue("ip").toString())
                && requestBody.containsKey("port")
                && requestBody.getValue("port") instanceof Integer;
    }

    public static JsonArray runFping(JsonArray devices) {

        var ipToIndexMap = new HashMap<String, Integer>();

        var ipList = new ArrayList<String>();

        // Step 1: Populate ipList and ipToIndexMap
        for (int i = 0; i < devices.size(); i++)
        {
            String ip = devices.getJsonObject(i).getString("ip");

            ipList.add(ip);

            ipToIndexMap.put(ip, i); // IP to index mapping
        }

        // Step 2: Run fping on all IPs
        var aliveIPs = new ArrayList<String>();

        var aliveDevices = new JsonArray();

        List<String> command = new ArrayList<>();
        command.add("fping");
        command.add("-c");
        command.add("3");
        command.add("-q");
        command.add("-t");
        command.add("500");
        command.add("-p");
        command.add("50");
        command.addAll(ipList);

        try
        {
            // Initialize ProcessBuilder
            var process = new ProcessBuilder(command).start();

            // Read from stderr (because fping -q outputs results to stderr)
            var reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            String line;

            while ((line = reader.readLine()) != null)
            {
                // Example line: "192.168.1.1 : xmt/rcv/%loss = 3/3/0%, min/avg/max = 1.01/1.23/1.45"
                if (!line.contains("100%"))
                {
                    aliveIPs.add(line.split(":")[0].trim());
                }
            }

            process.waitFor(5, TimeUnit.SECONDS);

        }
        catch (Exception e)
        {
            return aliveDevices;
        }

        for (var ip : aliveIPs)
        {
            aliveDevices.add(devices.getJsonObject(ipToIndexMap.get(ip)));
        }

        return aliveDevices;
    }


    public static String runGoPlugin(JsonArray devices , String mode)
    {
        var output = new StringBuilder();

        try
        {
            var process = new ProcessBuilder("go/ssh-plugin", mode).start();

            try (var writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream())))
            {
                writer.write(devices.toString());

                writer.flush();
            }

            try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream())))
            {
                String line;

                while ((line = reader.readLine()) != null)
                {
                    output.append(line);
                }
            }

            var exitCode = process.waitFor();

            if (exitCode != 0)
            {
                logger.warn("Plugin exited with non-zero code: {}",exitCode);

                return "";
            }
        }
        catch (Exception exception)
        {
            logger.error("Error during SSH discovery {}", exception.getMessage());

            return "";
        }

        return output.toString();
    }
}
