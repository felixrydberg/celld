# The Python handler of the actual-VM check (`check_python_handlers`).
# Bindings come from the deployment, not from this config: DB is D1, CACHE is KV.
from urllib.parse import urlparse

from workers import Response, WorkerEntrypoint

served = 0  # requests this interpreter has handled; a fresh VM starts at 1


def plain(value):
    return value.to_py() if hasattr(value, "to_py") else value


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        global served
        served += 1
        path = urlparse(request.url).path
        if path == "/hello":
            return Response.json({"hello": "python", "served": served})
        if path == "/kv":
            await self.env.CACHE.put("python", "kv-from-python")
            return Response.json({"value": plain(await self.env.CACHE.get("python"))})
        if path == "/d1":
            db = self.env.DB
            await db.exec("CREATE TABLE IF NOT EXISTS py (n INTEGER)")
            await db.prepare("INSERT INTO py (n) VALUES (?)").bind(served).run()
            count = await db.prepare("SELECT count(*) AS c FROM py").first("c")
            return Response.json({"count": plain(count)})
        if path == "/random":
            # Two VMs restored from one snapshot must not repeat these.
            import random
            import tempfile

            return Response.json(
                {"random": random.random(), "temp": next(tempfile._get_candidate_names())}
            )
        if path == "/fatal":
            # What Pyodide does when the interpreter cannot continue.
            import pyodide_js
            from js import Error

            pyodide_js._api.fatal_error(Error.new("deliberate fatal error"))
        return Response("not found", status=404)
