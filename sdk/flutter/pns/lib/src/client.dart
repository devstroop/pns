import 'dart:async';
import 'dart:convert';
import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

/// Client for the Devstroop Push service.
class PnsClient {
  PnsClient({required this.baseUrl, this.apiKey});

  final String baseUrl;
  final String? apiKey;

  Map<String, String> get _headers => {
        'Content-Type': 'application/json',
        if (apiKey != null && apiKey!.isNotEmpty) 'X-API-Key': apiKey!,
      };

  /// Register a device for a user.
  Future<void> registerDevice({
    required String userId,
    required String platform,
    required String transport,
    String? endpoint,
    String? token,
  }) async {
    final body = <String, dynamic>{
      'user_id': userId,
      'platform': platform,
      'transport': transport,
      if (endpoint != null) 'endpoint': endpoint,
      if (token != null) 'token': token,
    };
    final res = await http.post(
      Uri.parse('$baseUrl/devices'),
      headers: _headers,
      body: jsonEncode(body),
    );
    if (res.statusCode < 200 || res.statusCode >= 300) {
      throw Exception('registerDevice failed: ${res.statusCode} ${res.body}');
    }
  }

  /// Send a notification to a user's devices (server-side/admin use).
  Future<String> push({
    required String userId,
    required String title,
    required String body,
    String data = '',
  }) async {
    final res = await http.post(
      Uri.parse('$baseUrl/push'),
      headers: _headers,
      body: jsonEncode({
        'user_id': userId,
        'title': title,
        'body': body,
        'data': data,
      }),
    );
    if (res.statusCode < 200 || res.statusCode >= 300) {
      throw Exception('push failed: ${res.statusCode} ${res.body}');
    }
    final json = jsonDecode(res.body) as Map<String, dynamic>;
    return json['id'] as String? ?? '';
  }

  /// Poll pending notifications (fallback).
  Future<List<Map<String, dynamic>>> poll(String userId) async {
    final res = await http.get(
      Uri.parse('$baseUrl/poll/$userId'),
      headers: _headers,
    );
    if (res.statusCode < 200 || res.statusCode >= 300) {
      throw Exception('poll failed: ${res.statusCode} ${res.body}');
    }
    final json = jsonDecode(res.body) as Map<String, dynamic>;
    final list = json['notifications'] as List<dynamic>? ?? [];
    return list.whereType<Map>().map(Map<String, dynamic>.from).toList();
  }
}


extension PnsRealtime on PnsClient {
  Stream<Map<String, dynamic>> connect({required String token}) {
    final wsUrl = baseUrl.replaceFirst('http', 'ws');
    final channel = WebSocketChannel.connect(Uri.parse('$wsUrl/ws?token=$token'));
    return channel.stream.map((event) {
      try {
        if (event is String) {
          final decoded = jsonDecode(event);
          if (decoded is Map) return Map<String, dynamic>.from(decoded);
        }
      } catch (_) {}
      return <String, dynamic>{'raw': event.toString()};
    });
  }
}
