package luminar.ru.http;

import luminar.ru.lsm.LsmStorage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Неблокирующий HTTP-сервер на базе Java NIO (Single-Threaded Event Loop).
 * <p>
 * Поддерживаемые эндпоинты:
 * - GET    /get?key={key}
 * - POST   /set (Body: key={key}&value={value})
 * - DELETE /delete?key={key}
 */
public class NioHttpServer {

    private static final Logger log = Logger.getLogger(NioHttpServer.class.getName());

    private static final int PORT = 8080;
    private static final int BUFFER_SIZE = 8192;
    private static final long CONNECTION_TIMEOUT_MS = 30_000;

    private final LsmStorage storage;

    public NioHttpServer() {
        this.storage = new LsmStorage(Path.of("data"), 1000);
    }

    public static void main(String[] args) {
        NioHttpServer server = new NioHttpServer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Остановка сервера и безопасное закрытие хранилища...");
            server.storage.close();
        }));

        try {
            server.start();
        } catch (IOException e) {
            log.log(Level.SEVERE, "Критическая ошибка сервера", e);
        }
    }

    public void start() throws IOException {
        Selector selector = Selector.open();
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(PORT));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        log.info("NIO HTTP Сервер успешно запущен на порту " + PORT);

        while (true) {
            selector.select(1000);

            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = selectedKeys.iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove();
                processKey(key, selector);
            }

            checkTimeouts(selector);
        }
    }

    private void processKey(SelectionKey key, Selector selector) {
        try {
            if (!key.isValid()) return;
            if (key.isAcceptable()) accept(key, selector);
            else if (key.isReadable()) read(key);
            else if (key.isWritable()) write(key);
        } catch (IOException e) {
            log.warning("Разрыв соединения с клиентом: " + e.getMessage());
            closeQuietly(key);
        }
    }

    private void checkTimeouts(Selector selector) {
        long now = System.currentTimeMillis();

        for (SelectionKey key : selector.keys()) {
            if (key.isValid() && key.attachment() instanceof ConnectionContext ctx) {
                if (now - ctx.lastActivityTime > CONNECTION_TIMEOUT_MS) {
                    log.info("Таймаут соединения. Отключение клиента: " + getRemoteAddressQuietly((SocketChannel) key.channel()));
                    closeQuietly(key);
                }
            }
        }
    }

    private void accept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        if (client != null) {
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ, new ConnectionContext());
            log.fine("Новое подключение: " + client.getRemoteAddress());
        }
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ConnectionContext ctx = (ConnectionContext) key.attachment();
        ctx.updateActivity();

        switch (ctx.state) {
            case READ_HEADERS -> readHeaders(channel, ctx, key);
            case READ_BODY -> readBody(channel, ctx, key);
            default -> closeQuietly(key);
        }
    }

    private void readHeaders(SocketChannel channel, ConnectionContext ctx, SelectionKey key) throws IOException {
        int bytesRead = channel.read(ctx.readBuffer);
        ctx.updateActivity();

        if (bytesRead == -1) {
            closeQuietly(key);
            return;
        }
        if (bytesRead == 0) return;

        ctx.readBuffer.flip();
        int headerEnd = findEndOfHeaders(ctx.readBuffer);

        if (headerEnd != -1) {
            byte[] headerBytes = new byte[headerEnd];
            ctx.readBuffer.get(headerBytes);
            parseRequest(ctx, new String(headerBytes, StandardCharsets.US_ASCII));

            if (ctx.contentLength > 0) {
                ctx.state = RequestStatus.READ_BODY;
                ctx.bodyBuffer = ByteBuffer.allocate(ctx.contentLength);
                if (ctx.readBuffer.hasRemaining()) {
                    ctx.bodyBuffer.put(ctx.readBuffer);
                }
                ctx.readBuffer.clear();

                if (ctx.bodyBuffer.position() == ctx.contentLength) {
                    processRequest(ctx, key);
                }
            } else {
                ctx.readBuffer.clear();
                processRequest(ctx, key);
            }
        } else {
            if (ctx.readBuffer.limit() == ctx.readBuffer.capacity()) {
                sendError(ctx, key, 431, "Request Header Fields Too Large");
            } else {
                ctx.readBuffer.compact();
            }
        }
    }

    private void readBody(SocketChannel channel, ConnectionContext ctx, SelectionKey key) throws IOException {
        int bytesRead = channel.read(ctx.bodyBuffer);
        ctx.updateActivity();

        if (bytesRead == -1) {
            closeQuietly(key);
            return;
        }
        if (bytesRead == 0) return;

        if (ctx.bodyBuffer.position() == ctx.contentLength) {
            processRequest(ctx, key);
        }
    }

    private void processRequest(ConnectionContext ctx, SelectionKey key) {
        log.info("Обработка запроса: " + ctx.method + " " + ctx.path);
        try {
            if ("GET".equals(ctx.method) && ctx.path.startsWith("/get")) {
                handleGet(ctx);
            } else if (("POST".equals(ctx.method) || "PUT".equals(ctx.method)) && ctx.path.startsWith("/set")) {
                handleSet(ctx);
            } else if ("DELETE".equals(ctx.method) && ctx.path.startsWith("/delete")) {
                handleDelete(ctx);
            } else {
                sendError(ctx, key, 404, "Not Found");
                return;
            }
            key.interestOps(SelectionKey.OP_WRITE);
            ctx.state = RequestStatus.WRITE_RESPONSE;
        } catch (Exception e) {
            log.log(Level.SEVERE, "Внутренняя ошибка при обработке запроса", e);
            sendError(ctx, key, 500, "Internal Server Error");
        }
    }

    private void handleGet(ConnectionContext ctx) {
        String query = ctx.path.contains("?") ? ctx.path.substring(ctx.path.indexOf("?") + 1) : "";
        Map<String, String> params = parseFormParameters(query);
        String key = params.get("key");

        if (key != null) {
            String value = storage.get(key);
            if (value != null) {
                buildResponse(ctx, 200, "OK", value);
            } else {
                buildResponse(ctx, 404, "Not Found", "Key not found");
            }
        } else {
            sendError(ctx, null, 400, "Missing 'key' parameter");
        }
    }

    private void handleSet(ConnectionContext ctx) {
        if (ctx.bodyBuffer == null || ctx.bodyBuffer.position() == 0) {
            sendError(ctx, null, 400, "Body required");
            return;
        }

        ctx.bodyBuffer.flip();
        String body = StandardCharsets.UTF_8.decode(ctx.bodyBuffer).toString();
        Map<String, String> params = parseFormParameters(body);

        String key = params.get("key");
        String value = params.get("value");

        if (key != null && value != null) {
            storage.put(key, value);
            buildResponse(ctx, 200, "OK", "Saved");
        } else {
            sendError(ctx, null, 400, "Invalid format. Use key=...&value=...");
        }
    }

    /**
     * Удаляет ключ из хранилища.
     * Записывает tombstone через LsmStorage.delete().
     * Возвращает 200 даже если ключ не существовал (идемпотентное поведение DELETE).
     */
    private void handleDelete(ConnectionContext ctx) {
        String query = ctx.path.contains("?") ? ctx.path.substring(ctx.path.indexOf("?") + 1) : "";
        Map<String, String> params = parseFormParameters(query);
        String key = params.get("key");

        if (key == null || key.isBlank()) {
            sendError(ctx, null, 400, "Missing 'key' parameter");
            return;
        }

        storage.delete(key);
        buildResponse(ctx, 200, "OK", "Deleted");
    }

    private Map<String, String> parseFormParameters(String queryOrBody) {
        Map<String, String> params = new HashMap<>();
        if (queryOrBody == null || queryOrBody.isBlank()) return params;

        String[] pairs = queryOrBody.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            if (idx > 0) {
                String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                params.put(key, value);
            }
        }
        return params;
    }

    private void write(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ConnectionContext ctx = (ConnectionContext) key.attachment();

        channel.write(ctx.responseBuffer);
        ctx.updateActivity();

        if (!ctx.responseBuffer.hasRemaining()) {
            if (ctx.closeAfterResponse) {
                log.fine("Закрытие соединения после отправки ошибочного ответа.");
                closeQuietly(key);
            } else {
                ctx.reset();
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }

    private void parseRequest(ConnectionContext ctx, String headersStr) {
        String[] lines = headersStr.split("\r\n");
        if (lines.length > 0) {
            String[] parts = lines[0].split(" ");
            if (parts.length >= 2) {
                ctx.method = parts[0];
                ctx.path = parts[1];
            }
        }
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].toLowerCase();
            if (line.startsWith("content-length:")) {
                String[] headerParts = line.split(":", 2);
                if (headerParts.length == 2) {
                    try {
                        ctx.contentLength = Integer.parseInt(headerParts[1].trim());
                    } catch (NumberFormatException e) {
                        ctx.contentLength = 0;
                    }
                }
            }
        }
    }

    private void buildResponse(ConnectionContext ctx, int code, String status, String body) {
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        String connectionPolicy = ctx.closeAfterResponse ? "close" : "keep-alive";

        String response = "HTTP/1.1 " + code + " " + status + "\r\n" +
                "Content-Type: text/plain; charset=utf-8\r\n" +
                "Content-Length: " + bodyBytes.length + "\r\n" +
                "Connection: " + connectionPolicy + "\r\n\r\n";

        byte[] headerBytes = response.getBytes(StandardCharsets.UTF_8);

        ByteBuffer resp = ByteBuffer.allocate(headerBytes.length + bodyBytes.length);
        resp.put(headerBytes);
        resp.put(bodyBytes);
        resp.flip();
        ctx.responseBuffer = resp;
    }

    private void sendError(ConnectionContext ctx, SelectionKey key, int code, String message) {
        ctx.closeAfterResponse = true;
        buildResponse(ctx, code, message, message);

        ctx.state = RequestStatus.WRITE_RESPONSE;
        if (key != null) {
            key.interestOps(SelectionKey.OP_WRITE);
        }
    }

    private int findEndOfHeaders(ByteBuffer buffer) {
        int limit = buffer.limit();
        for (int i = buffer.position(); i < limit - 3; i++) {
            if (buffer.get(i) == '\r' && buffer.get(i + 1) == '\n' &&
                    buffer.get(i + 2) == '\r' && buffer.get(i + 3) == '\n') {
                return i + 4;
            }
        }
        return -1;
    }

    private void closeQuietly(SelectionKey key) {
        if (key != null) {
            try {
                key.channel().close();
            } catch (IOException ignored) {
            }
            key.cancel();
        }
    }

    private String getRemoteAddressQuietly(SocketChannel channel) {
        try {
            return channel.getRemoteAddress().toString();
        } catch (IOException e) {
            return "Unknown";
        }
    }

    enum RequestStatus {READ_HEADERS, READ_BODY, WRITE_RESPONSE}

    static class ConnectionContext {
        ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        ByteBuffer responseBuffer;
        ByteBuffer bodyBuffer;

        RequestStatus state = RequestStatus.READ_HEADERS;
        String method;
        String path;
        int contentLength = 0;

        long lastActivityTime = System.currentTimeMillis();
        boolean closeAfterResponse = false;

        void updateActivity() {
            this.lastActivityTime = System.currentTimeMillis();
        }

        void reset() {
            readBuffer.clear();
            responseBuffer = null;
            bodyBuffer = null;
            state = RequestStatus.READ_HEADERS;
            method = null;
            path = null;
            contentLength = 0;
            closeAfterResponse = false;
        }
    }
}