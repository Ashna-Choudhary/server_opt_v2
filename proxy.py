from fastapi import FastAPI
from pydantic import BaseModel
import httpx
import asyncio
from collections import deque
import threading

CLASSIFICATION_SERVER_URL = "http://localhost:8001/classify"

app = FastAPI(
    title="Optimized Classification Proxy",
    description="High-performance proxy with intelligent batching for the code classification service"
)

class ProxyRequest(BaseModel):
    """Request model for single text classification"""
    sequence: str

class ProxyResponse(BaseModel):
    """Response model containing classification result ('code' or 'not code')"""
    result: str

# Advanced priority-based batching system
small_requests = deque()  # <= 12 chars (fast processing)
medium_requests = deque()  # 13-20 chars
large_requests = deque()  # > 20 chars (slow processing)
pending_lock = threading.Lock()
batch_semaphore = asyncio.Semaphore(1)  # Ensure only one batch processes at a time

async def process_batch():
    """Ultra-fast length-aware priority batch processor"""
    while True:
        batch = []
        
        # Advanced batching strategy for optimal performance
        with pending_lock:
            # Strategy 1: Immediate full batches of small requests
            if len(small_requests) >= 5:
                for _ in range(5):
                    batch.append(small_requests.popleft())
            
            # Strategy 2: Aggressive mixed batching (don't wait)
            elif large_requests and len(small_requests) >= 1:
                batch.append(large_requests.popleft())  # One slow request
                while len(batch) < 5 and small_requests:  # Fill with fast ones
                    batch.append(small_requests.popleft())
            
            # Strategy 3: Medium requests in smaller groups (don't wait for 3)
            elif len(medium_requests) >= 2:
                for _ in range(min(4, len(medium_requests))):
                    batch.append(medium_requests.popleft())
            
            # Strategy 4: Take anything available (ultra-aggressive)
            else:
                all_queues = [small_requests, medium_requests, large_requests]
                for queue in all_queues:
                    while len(batch) < 5 and queue:
                        batch.append(queue.popleft())
                    if batch:  # Don't wait if we have something
                        break
        
        if batch:
            sequences = [req['sequence'] for req in batch]
            futures = [req['future'] for req in batch]
            
            try:
                # Use semaphore to prevent concurrent batch processing
                async with batch_semaphore:
                    # Reduce timeout for faster failure recovery
                    async with httpx.AsyncClient(timeout=6.0) as client:
                        response = await client.post(
                            CLASSIFICATION_SERVER_URL, 
                            json={"sequences": sequences}
                        )
                        
                        if response.status_code == 200:
                            results = response.json()["results"]
                            for i, future in enumerate(futures):
                                if i < len(results) and not future.done():
                                    future.set_result(results[i])
                        else:
                            for future in futures:
                                if not future.done():
                                    future.set_result("not code")
                                    
            except Exception as e:
                print(f"Batch processing error: {e}")
                for future in futures:
                    if not future.done():
                        future.set_result("not code")
        
        # Ultra-aggressive batching - immediate processing when possible
        if not batch:
            await asyncio.sleep(0.0005)  # 0.5ms when no work
        # No sleep when we have work - process immediately

# Start the batch processor
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_batch())

@app.post("/proxy_classify")
async def proxy_classify(req: ProxyRequest):
    """
    Ultra-optimized proxy with length-aware priority batching.
    
    Advanced optimizations:
    - Length-based request prioritization (small/medium/large queues)
    - Smart batching strategies for optimal server utilization
    - 1ms batch formation cycles for maximum responsiveness
    - Intelligent request packing to minimize processing time
    """
    # Create a future for this request
    future = asyncio.Future()
    
    # Categorize by string length for optimal batching
    seq_len = len(req.sequence)
    request_obj = {
        'sequence': req.sequence,
        'future': future
    }
    
    with pending_lock:
        if seq_len <= 12:
            small_requests.append(request_obj)
            # Immediate processing trigger for small requests (lower threshold)
            if len(small_requests) >= 4:
                asyncio.create_task(process_immediate_batch())
        elif seq_len <= 20:
            medium_requests.append(request_obj)
            # Process medium requests when we have 2 (more aggressive)
            if len(medium_requests) >= 2:
                asyncio.create_task(process_immediate_batch())
        else:
            large_requests.append(request_obj)
            # Process large requests immediately with any small padding
            if large_requests and len(small_requests) >= 1:
                asyncio.create_task(process_immediate_batch())

async def process_immediate_batch():
    """Process a batch immediately when triggered"""
    batch = []
    
    with pending_lock:
        # Quick batch formation
        if len(small_requests) >= 4:
            for _ in range(min(5, len(small_requests))):
                batch.append(small_requests.popleft())
        elif large_requests and len(small_requests) >= 2:
            batch.append(large_requests.popleft())
            for _ in range(min(4, len(small_requests))):
                batch.append(small_requests.popleft())
        elif len(medium_requests) >= 3:
            for _ in range(min(3, len(medium_requests))):
                batch.append(medium_requests.popleft())
    
    if batch:
        sequences = [req['sequence'] for req in batch]
        futures = [req['future'] for req in batch]
        
        try:
            # Use semaphore to prevent concurrent batch processing
            async with batch_semaphore:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.post(
                        CLASSIFICATION_SERVER_URL, 
                        json={"sequences": sequences}
                    )
                    
                    if response.status_code == 200:
                        results = response.json()["results"]
                        for i, future in enumerate(futures):
                            if i < len(results) and not future.done():
                                future.set_result(results[i])
                    else:
                        for future in futures:
                            if not future.done():
                                future.set_result("not code")
                                
        except Exception as e:
            print(f"Immediate batch error: {e}")
            for future in futures:
                if not future.done():
                    future.set_result("not code")

@app.post("/proxy_classify")
async def proxy_classify(req: ProxyRequest):
    """
    Ultra-optimized proxy with length-aware priority batching.
    
    Advanced optimizations:
    - Length-based request prioritization (small/medium/large queues)
    - Smart batching strategies for optimal server utilization
    - 0.5ms batch formation cycles for maximum responsiveness
    - Intelligent request packing to minimize processing time
    """
    # Create a future for this request
    future = asyncio.Future()
    
    # Categorize by string length for optimal batching
    seq_len = len(req.sequence)
    request_obj = {
        'sequence': req.sequence,
        'future': future
    }
    
    with pending_lock:
        if seq_len <= 12:
            small_requests.append(request_obj)
            # Immediate processing trigger for small requests (lower threshold)
            if len(small_requests) >= 4:
                asyncio.create_task(process_immediate_batch())
        elif seq_len <= 20:
            medium_requests.append(request_obj)
            # Process medium requests when we have 2 (more aggressive)
            if len(medium_requests) >= 2:
                asyncio.create_task(process_immediate_batch())
        else:
            large_requests.append(request_obj)
            # Process large requests immediately with any small padding
            if large_requests and len(small_requests) >= 1:
                asyncio.create_task(process_immediate_batch())
    
    # Wait for result with shorter timeout for faster processing
    try:
        result = await asyncio.wait_for(future, timeout=8.0)
        return ProxyResponse(result=result)
    except asyncio.TimeoutError:
        return ProxyResponse(result="not code")
