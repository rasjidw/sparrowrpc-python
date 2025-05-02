# Project Roadmap

## Stage 1 - Complete the core API and transports

Transports planned include:

 - TCP/IP - DONE
 - Websocket - DONE
 - Stdio - DONE
 - file based (for debugging, develpment and testing)
 
 
## Stage 2 - Optional / Additional features

 - Fill out 'node' support - allowing each peer to 'export' targets, which in turn allows 'hub and spoke' topology (like WAMP) if so desired.
 - Additional transports
   - windows pipes / unix socket etc
   - shared memory (instead of stdio)
 