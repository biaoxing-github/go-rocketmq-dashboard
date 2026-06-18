package dev.codex.rocketmq;

import com.alibaba.fastjson2.JSON;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.rocketmq.tools.command.MQAdminStartup;

// AdminSidecar 在常驻 JVM 内执行 RocketMQ 官方 mqadmin 子命令，避免 Go 服务每次查询都拉起 Java 进程。
public final class AdminSidecar {
    private static final Object COMMAND_LOCK = new Object();

    private AdminSidecar() {
    }

    // main 读取监听地址并启动两个极窄 HTTP 接口：健康检查和命令执行。
    public static void main(String[] args) throws Exception {
        String addr = argValue(args, "--addr", envOrDefault("RMQD_ADMIN_SIDECAR_ADDR", "127.0.0.1:18091"));
        HttpServer server = HttpServer.create(parseAddress(addr), 0);
        server.createContext("/health", AdminSidecar::handleHealth);
        server.createContext("/run", AdminSidecar::handleRun);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.printf("RocketMQ admin sidecar listening on %s%n", addr);
    }

    // handleHealth 让 Go 进程和 Docker 健康等待能确认 sidecar 已经完成启动。
    private static void handleHealth(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            writeJSON(exchange, 405, Map.of("error", "method not allowed"));
            return;
        }
        writeJSON(exchange, 200, Map.of("status", "ok"));
    }

    // handleRun 解析 Go 端传来的命令参数，并返回官方 mqadmin 输出文本。
    private static void handleRun(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            writeJSON(exchange, 405, Map.of("error", "method not allowed"));
            return;
        }
        try {
            byte[] body = exchange.getRequestBody().readAllBytes();
            RunRequest request = JSON.parseObject(new String(body, StandardCharsets.UTF_8), RunRequest.class);
            if (request == null || request.args == null) {
                writeJSON(exchange, 400, Map.of("error", "args required"));
                return;
            }
            RunResult result = runMQAdmin(request.args);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("output", result.output);
            if (!result.files.isEmpty()) {
                response.put("files", result.files);
            }
            writeJSON(exchange, 200, response);
        } catch (Exception ex) {
            Map<String, String> response = new LinkedHashMap<>();
            response.put("error", ex.getClass().getSimpleName() + ": " + ex.getMessage());
            writeJSON(exchange, 500, response);
        }
    }

    // runMQAdmin 同步捕获 stdout/stderr 和相对输出文件，避免多个 HTTP 请求并发时输出或文件互相串线。
    private static RunResult runMQAdmin(List<String> args) throws Exception {
        synchronized (COMMAND_LOCK) {
            clearRegisteredCommands();
            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            try (PrintStream capture = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
                System.setOut(capture);
                System.setErr(capture);
                MQAdminStartup.main0(args.toArray(new String[0]), null);
            } finally {
                System.setOut(originalOut);
                System.setErr(originalErr);
            }
            String output = buffer.toString(StandardCharsets.UTF_8);
            return new RunResult(output, collectCommandOutputFiles(args, output));
        }
    }

    // collectCommandOutputFiles 读取官方命令声明的相对输出文件，由 Go 调用方在自己的工作目录落盘。
    private static List<OutputFile> collectCommandOutputFiles(List<String> args, String output) throws IOException {
        java.util.ArrayList<OutputFile> files = new java.util.ArrayList<>();
        if (!isConsumerStatusListCommand(args)) {
            return files;
        }
        Path sidecarRoot = new File("").getAbsoluteFile().toPath();
        for (String relativePath : consumerStatusOutputFiles(output)) {
            Path source = sidecarRoot.resolve(relativePath).normalize();
            if (!Files.exists(source)) {
                throw new IOException("consumerStatus output file missing: " + source);
            }
            files.add(new OutputFile(relativePath, Files.readString(source, StandardCharsets.UTF_8)));
            Files.delete(source);
            deleteEmptyParents(source.getParent(), sidecarRoot);
        }
        return files;
    }

    // consumerStatusOutputFiles 解析官方 consumerStatus 表格最后一列，最后一列就是 ConsumerRunningInfoFile 相对路径。
    private static List<String> consumerStatusOutputFiles(String output) {
        java.util.ArrayList<String> files = new java.util.ArrayList<>();
        if (output == null || output.isBlank()) {
            return files;
        }
        for (String line : output.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            String[] columns = trimmed.split("\\s+");
            if (columns.length >= 4 && isPositiveInteger(columns[0])) {
                String filePath = columns[columns.length - 1];
                if (filePath.contains("/") || filePath.contains("\\")) {
                    files.add(filePath);
                }
            }
        }
        return files;
    }

    private static boolean isConsumerStatusListCommand(List<String> args) {
        return args != null && !args.isEmpty()
            && "consumerStatus".equalsIgnoreCase(args.get(0))
            && !hasOption(args, "-i", "--clientId");
    }

    private static boolean hasOption(List<String> args, String shortName, String longName) {
        for (String arg : args) {
            if (shortName.equals(arg) || longName.equals(arg)
                || arg.startsWith(shortName + "=") || arg.startsWith(longName + "=")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPositiveInteger(String value) {
        for (int index = 0; index < value.length(); index++) {
            char ch = value.charAt(index);
            if (ch < '0' || ch > '9') {
                return false;
            }
        }
        return !value.isEmpty() && !"0".equals(value);
    }

    private static void deleteEmptyParents(Path start, Path stop) throws IOException {
        Path current = start;
        while (current != null && !current.equals(stop) && Files.isDirectory(current)) {
            try (java.util.stream.Stream<Path> entries = Files.list(current)) {
                if (entries.findAny().isPresent()) {
                    return;
                }
            }
            Files.delete(current);
            current = current.getParent();
        }
    }

    // clearRegisteredCommands 清理 MQAdminStartup.main0 每次 initCommand 累加的静态列表，避免常驻进程长期膨胀。
    @SuppressWarnings("unchecked")
    private static void clearRegisteredCommands() throws Exception {
        Field field = MQAdminStartup.class.getDeclaredField("SUB_COMMANDS");
        field.setAccessible(true);
        List<Object> commands = (List<Object>) field.get(null);
        commands.clear();
    }

    private static void writeJSON(HttpExchange exchange, int status, Object payload) throws IOException {
        byte[] response = JSON.toJSONString(payload).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private static InetSocketAddress parseAddress(String raw) {
        String value = raw == null || raw.isBlank() ? "127.0.0.1:18091" : raw.trim();
        int colon = value.lastIndexOf(':');
        if (colon < 0) {
            return new InetSocketAddress(value, 18091);
        }
        String host = value.substring(0, colon);
        int port = Integer.parseInt(value.substring(colon + 1));
        if (host.isBlank()) {
            host = "0.0.0.0";
        }
        return new InetSocketAddress(host, port);
    }

    private static String argValue(String[] args, String name, String fallback) {
        for (int index = 0; index + 1 < args.length; index++) {
            if (name.equals(args[index])) {
                return args[index + 1];
            }
        }
        return fallback;
    }

    private static String envOrDefault(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    // RunRequest 是 /run 请求体，args 与 mqadmin 子命令参数一一对应。
    public static final class RunRequest {
        public List<String> args;
    }

    // RunResult 保存一次官方 mqadmin 调用的文本输出和需要回传给 Go 端落盘的文件。
    public static final class RunResult {
        // output 是官方 mqadmin 捕获到的 stdout/stderr 合并文本。
        public final String output;
        // files 是官方命令生成的相对路径文件列表，当前用于 consumerStatus 列表模式。
        public final List<OutputFile> files;

        RunResult(String output, List<OutputFile> files) {
            this.output = output;
            this.files = files;
        }
    }

    // OutputFile 表示一个官方命令生成的相对路径文本文件。
    public static final class OutputFile {
        // path 是相对 sidecar 工作目录的文件路径，Go 端会按同一相对路径写入调用方工作目录。
        public final String path;
        // content 是该文件的完整文本内容。
        public final String content;

        OutputFile(String path, String content) {
            this.path = path;
            this.content = content;
        }
    }
}
