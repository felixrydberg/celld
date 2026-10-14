"""Touched Python heap allocations for the trusted T33 memory baseline."""
from urllib.parse import urlparse
from workers import Response, WorkerEntrypoint

served = 0
retained = None


def allocate():
    value = bytearray(8 * 1024 * 1024)
    for offset in range(0, len(value), 4096):
        value[offset] = 11
    return value


def inspect(value):
    return {"bytes": len(value) if value is not None else 0,
            "checksum": sum(value[::4096]) if value is not None else 0}


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        global served, retained
        served += 1
        path = urlparse(request.url).path
        if path == "/temporary":
            measured = inspect(allocate())
        else:
            if path == "/hold":
                retained = allocate()
            elif path == "/release":
                retained = None
            elif path not in ("/hello", "/status"):
                return Response("not found", status=404)
            measured = inspect(retained)
        return Response.json({"served": served, **measured})
