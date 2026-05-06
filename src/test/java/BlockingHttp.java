import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Простой блокирующий HTTP-сервер для тестирования.
 * Отвечает на GET /hello и возвращает 404 для остальных запросов.
 */
public class BlockingHttp {

    private static final int PORT = 8080;

    /**
     * Запускает сервер, принимающий подключения в бесконечном цикле.
     */
    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Сервер запущен на порту " + PORT);
            System.out.println("Ожидание подключений...");

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    System.out.println("\n--- Новое подключение от " + socket.getInetAddress() + " ---");
                    handleClient(socket);
                } catch (IOException e) {
                    System.err.println("Ошибка при обработке клиента: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Не удалось запустить сервер: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void handleClient(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             OutputStream rawOut = socket.getOutputStream()) {

            String requestLine = in.readLine();
            if (requestLine == null) {
                System.out.println("Клиент закрыл соединение, не отправив запрос");
                return;
            }
            System.out.println("Request Line: " + requestLine);

            List<String> headers = new ArrayList<>();
            String headerLine;
            while ((headerLine = in.readLine()) != null && !headerLine.isEmpty()) {
                headers.add(headerLine);
                System.out.println("Header: " + headerLine);
            }
            System.out.println("Заголовки прочитаны, всего: " + headers.size());

            String[] parts = requestLine.split(" ");
            if (parts.length < 3) {
                sendResponse(rawOut, 400, "Bad Request", "Invalid request line");
                return;
            }

            String method = parts[0];
            String path = parts[1];

            System.out.println("Метод: " + method + ", Путь: " + path);

            if (!"GET".equalsIgnoreCase(method)) {
                sendResponse(rawOut, 405, "Method Not Allowed", "Only GET is supported");
                return;
            }

            if ("/hello".equals(path)) {
                sendResponse(rawOut, 200, "OK", "Hello, World!");
            } else {
                sendResponse(rawOut, 404, "Not Found", "Page not found");
            }

            System.out.println("Ответ отправлен, соединение закрыто");

        } catch (IOException e) {
            System.err.println("Ошибка ввода-вывода при обработке клиента: " + e.getMessage());
        }
    }

    /**
     * Отправляет HTTP-ответ с заданным статусом и телом.
     */
    private static void sendResponse(OutputStream out, int statusCode, String statusText, String body) throws IOException {
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        String header = "HTTP/1.1 " + statusCode + " " + statusText + "\r\n" +
                "Content-Type: text/plain; charset=UTF-8\r\n" +
                "Content-Length: " + bodyBytes.length + "\r\n" +
                "Connection: close\r\n\r\n";
        out.write(header.getBytes(StandardCharsets.UTF_8));
        out.write(bodyBytes);
        out.flush();
    }
}