import asyncio
import aiohttp
import time
import statistics

BASE_URL = "http://localhost:8000"
TOTAL_TASKS = 100
CONCURRENCY = 10

async def enqueue_task(session, task_id):
    payload = {
        "task_type": "benchmark",
        "payload": {"task_id": task_id, "data": f"benchmark_task_{task_id}"},
        "priority": 1
    }
    start = time.perf_counter()
    try:
        async with session.post(f"{BASE_URL}/tasks/", json=payload) as resp:
            elapsed = time.perf_counter() - start
            status = resp.status
            return {"task_id": task_id, "status": status, "latency": elapsed, "success": status in (200, 201)}
    except Exception as e:
        elapsed = time.perf_counter() - start
        return {"task_id": task_id, "status": 0, "latency": elapsed, "success": False, "error": str(e)}

async def run_benchmark():
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=30)
    
    print(f"FlowQ Benchmark — {TOTAL_TASKS} tasks, concurrency {CONCURRENCY}")
    print("-" * 50)
    
    results = []
    overall_start = time.perf_counter()
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        semaphore = asyncio.Semaphore(CONCURRENCY)
        
        async def bounded(task_id):
            async with semaphore:
                return await enqueue_task(session, task_id)
        
        tasks = [bounded(i) for i in range(1, TOTAL_TASKS + 1)]
        results = await asyncio.gather(*tasks)
    
    overall_elapsed = time.perf_counter() - overall_start
    
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]
    latencies = [r["latency"] * 1000 for r in successes]
    
    throughput = len(successes) / overall_elapsed
    
    print(f"Total tasks:      {TOTAL_TASKS}")
    print(f"Successful:       {len(successes)}")
    print(f"Failed:           {len(failures)}")
    print(f"Success rate:     {len(successes)/TOTAL_TASKS*100:.1f}%")
    print(f"Total time:       {overall_elapsed:.2f}s")
    print(f"Throughput:       {throughput:.1f} req/s")
    if latencies:
        print(f"Latency (avg):    {statistics.mean(latencies):.1f} ms")
        print(f"Latency (p50):    {statistics.median(latencies):.1f} ms")
        print(f"Latency (p95):    {sorted(latencies)[int(len(latencies)*0.95)]:.1f} ms")
        print(f"Latency (p99):    {sorted(latencies)[int(len(latencies)*0.99)]:.1f} ms")
        print(f"Latency (min):    {min(latencies):.1f} ms")
        print(f"Latency (max):    {max(latencies):.1f} ms")
    print("-" * 50)
    if failures:
        print("Failures:", [r.get("error","non-2xx") for r in failures[:5]])

if __name__ == "__main__":
    asyncio.run(run_benchmark())
