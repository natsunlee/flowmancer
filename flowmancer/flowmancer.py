import sys, asyncio
from .executor import Executor
from .jobspec.yaml import YAML
from .watchers.monitor import Monitor
from .watchers.progressbar import ProgressBar

def build_executors(filename: str):
    y = YAML()
    job = y.load(filename).dict()
    executors = { name:Executor(name) for name in job["tasks"] }
    for task, detl in job["tasks"].items():
        ex = executors[task]
        ex.module = detl["module"]
        ex.task = detl["task"]
        for d in (detl.get("dependencies") or []):
            if d not in executors:
                raise ValueError(f"Dependency '{d}' does not exist.")
            executors[task].add_dependency(executors[d])
    return executors

async def main(args):
    filename = args[0]
    executors = build_executors(filename)
    tasks = [
        asyncio.create_task(ex.start())
        for _, ex in executors.items()
    ]
    tasks.append(ProgressBar(executors).start())
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main(sys.argv))