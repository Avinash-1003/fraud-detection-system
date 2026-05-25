"""
WebSocket Router
================
Real-time push updates to the frontend dashboard.
Reads from the simulation results file and pushes new events.
"""

import os
import json
import asyncio
import logging
from typing import List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger("websocket")

router = APIRouter(tags=["WebSocket"])


class ConnectionManager:
    """Manages WebSocket connections for broadcasting."""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Active: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Active: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Send a message to all connected clients."""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)

        # Clean up dead connections
        for conn in disconnected:
            self.active_connections.remove(conn)


manager = ConnectionManager()


@router.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for live transaction feed.
    Reads new results from the simulation output file and pushes them.
    """
    await manager.connect(websocket)

    results_file = os.getenv(
        "SIMULATION_RESULTS",
        os.path.join(os.path.dirname(__file__), "..", "simulation_results.jsonl")
    )

    try:
        # If file exists, tail it for new entries
        if os.path.exists(results_file):
            with open(results_file, "r") as f:
                # Seek to end
                f.seek(0, 2)

                while True:
                    line = f.readline()
                    if line:
                        try:
                            data = json.loads(line.strip())
                            await manager.broadcast({
                                "type": "transaction",
                                "data": data
                            })
                        except json.JSONDecodeError:
                            pass
                    else:
                        await asyncio.sleep(0.2)  # Poll interval

                    # Check for client messages (keepalive/disconnect)
                    try:
                        await asyncio.wait_for(
                            websocket.receive_text(), timeout=0.01
                        )
                    except asyncio.TimeoutError:
                        pass
        else:
            # No file — send demo data periodically
            import random
            from datetime import datetime

            while True:
                # Generate synthetic demo event for dashboard testing
                demo_event = {
                    "type": "transaction",
                    "data": {
                        "transaction_id": f"TXN-DEMO-{random.randint(100000, 999999)}",
                        "cardholder_id": f"CH-{random.randint(10000, 10499)}",
                        "amount": round(random.uniform(10, 5000), 2),
                        "merchant_name": random.choice([
                            "Amazon", "Flipkart", "Swiggy", "Shell Petrol",
                            "Tanishq", "Netflix", "Uber", "DMart"
                        ]),
                        "channel": random.choice(["POS", "ONLINE", "MOBILE", "ATM"]),
                        "fraud_score": round(random.uniform(0, 1), 4),
                        "classification": random.choices(
                            ["LEGITIMATE", "SUSPICIOUS", "FRAUDULENT"],
                            weights=[85, 10, 5], k=1
                        )[0],
                        "processing_time_ms": round(random.uniform(0.5, 5), 2),
                        "timestamp": int(datetime.now().timestamp() * 1000),
                    }
                }
                await manager.broadcast(demo_event)
                await asyncio.sleep(1)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)
