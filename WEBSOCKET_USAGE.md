# WebSocket Support for acp-bridge

## Overview

acp-bridge now supports WebSocket transport in addition to the default stdin/stdout ACP mode. This allows remote clients to connect to the AI agent over a network connection.

## Usage

### Starting WebSocket Server

```bash
# Default (localhost:8765)
acp-bridge.exe --ws

# Custom host and port
set WS_HOST=0.0.0.0
set WS_PORT=8765
acp-bridge.exe --ws

# Or on Linux/Mac
export WS_HOST=0.0.0.0
export WS_PORT=8765
./acp-bridge --ws
```

### Environment Variables

- `WS_HOST` - WebSocket bind host (default: `127.0.0.1`)
- `WS_PORT` - WebSocket bind port (default: `8765`)
- `LLM_BASE_URL` - LLM backend URL
- `LLM_MODEL` - Model name to use
- `LLM_API_KEY` - API key (if required)
- `LLM_TIMEOUT` - Request timeout in seconds

### Client Connection Example

Connect to the WebSocket server at `ws://localhost:8765` and send JSON-RPC 2.0 messages:

```javascript
// Initialize
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {}
}

// Create new session
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "session/new",
  "params": {
    "cwd": "/path/to/working/dir"
  }
}

// Send prompt
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "session/prompt",
  "params": {
    "sessionId": "your-session-id",
    "prompt": "What files are in the current directory?"
  }
}
```

### Notifications

The server will send notifications during tool execution:

```javascript
// Progress notification
{
  "jsonrpc": "2.0",
  "method": "session/progress",
  "params": {
    "sessionId": "your-session-id",
    "progress": 0.5,
    "message": "Executing tool..."
  }
}

// Tool output
{
  "jsonrpc": "2.0",
  "method": "session/toolOutput",
  "params": {
    "sessionId": "your-session-id",
    "output": "file.txt"
  }
}
```

## Building on Windows

Run the included batch file:

```batch
build.bat
```

The executable will be created at `target\release\acp-bridge.exe`.

## Security Notes

- By default, the WebSocket server binds to `127.0.0.1` (localhost only)
- To accept remote connections, set `WS_HOST=0.0.0.0`
- When exposing to a network, ensure proper firewall rules are in place
- Consider using a reverse proxy with TLS for production deployments

## Protocol Details

The WebSocket transport uses the same ACP (Agent Client Protocol) JSON-RPC 2.0 messages as the stdin/stdout mode. All standard ACP methods are supported:

- `initialize` - Get agent capabilities
- `session/new` - Create a new session
- `session/prompt` - Send a prompt to the agent
- `session/end` - End a session
- `session/cancel` - Cancel a running session (notification)

Unsupported methods return error code `-32601` (Method Not Found):
- `session/load` / `session/resume` - No persistence layer
- `session/set_mode` - Sessions created without modes

## Example Python Client

```python
import asyncio
import websockets
import json

async def test_acp():
    async with websockets.connect("ws://localhost:8765") as ws:
        # Initialize
        await ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {}
        }))
        response = await ws.recv()
        print("Initialize:", response)
        
        # Create session
        await ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {"cwd": "."}
        }))
        response = await ws.recv()
        print("Session:", response)
        session_id = json.loads(response)["result"]["sessionId"]
        
        # Send prompt
        await ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": "List files in current directory"
            }
        }))
        
        # Receive notifications and final response
        while True:
            response = await ws.recv()
            data = json.loads(response)
            print("Response:", data)
            if "result" in data or "error" in data:
                break

asyncio.run(test_acp())
```
