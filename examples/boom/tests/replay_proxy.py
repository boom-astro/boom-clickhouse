"""Loopback-only HTTP fault gate for the disposable replay benchmark."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import urllib.error
import urllib.request


@contextmanager
def fault_gate(upstream_port):
    unavailable = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def respond(self, status, body):
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if self.path == "/__boom_replay_fault":
                if body == b"unavailable":
                    unavailable.set()
                elif body == b"healthy":
                    unavailable.clear()
                else:
                    self.respond(400, b"invalid replay control")
                    return
                self.respond(200, b"boom-replay-fault-accepted")
                return
            if unavailable.is_set():
                self.respond(503, b"intentional replay sink outage")
                return
            headers = {key: self.headers[key] for key in ["Content-Type", "Authorization"] if key in self.headers}
            request = urllib.request.Request(f"http://127.0.0.1:{upstream_port}{self.path}", data=body, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=35) as response:
                    self.respond(response.status, response.read())
            except urllib.error.HTTPError as error:
                self.respond(error.code, error.read())
            except (urllib.error.URLError, TimeoutError) as error:
                self.respond(502, str(error).encode())

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
