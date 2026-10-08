"""Cancel restored command work before privacy deletion starts its store stages."""
import asyncio
from contextlib import asynccontextmanager

class InteractionTasks:
    def __init__(self):
        self.active = {}

    @asynccontextmanager
    async def track(self, uid):
        task = asyncio.current_task()
        self.active.setdefault(uid, set()).add(task)
        try:
            yield
        finally:
            self.active.get(uid, set()).discard(task)
            if not self.active.get(uid):
                self.active.pop(uid, None)

    async def forget(self, uid):
        pending = [t for t in self.active.get(uid, ()) if t is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

WORK = InteractionTasks()
