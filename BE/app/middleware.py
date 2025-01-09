from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

class IPWhitelistMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, allowed_ips: list):
        super().__init__(app)
        self.allowed_ips = allowed_ips

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host
        if client_ip not in self.allowed_ips:
            return Response("Forbidden", status_code=403)
        try:
            response = await call_next(request)
        except Exception as e:
            response = Response(f"ERROR: Exception {str(type(e))}", status_code=500)
        return response