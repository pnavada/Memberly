# Memberly

## Contributors
Pruthvi Prakash Navada

## Overview
This project implements a distributed peer-to-peer system with membership management, leader election, and fault tolerance. The system utilizes both TCP and UDP for communication, implements heartbeat-based failure detection, and maintains consistency through a view-based approach.

## Directory Structure
```
.
├── Dockerfile
├── docker-compose-testcase-1.yml
├── docker-compose-testcase-2.yml
├── docker-compose-testcase-3.yml
├── docker-compose-testcase-4.yml
├── hostsfile.txt
├── main.go
└── peer/
    ├── config/
    │   └── config.go
    ├── datastructures/
    │   ├── safelist.go
    │   └── safevalue.go
    ├── handlers/
    │   └── messagehandler.go
    ├── network/
    │   ├── connection.go
    │   └── peer.go
    ├── types/
    │   └── types.go
    └── utils/
        └── utils.go
```

## Building and Running

### Prerequisites
- Docker
- Docker Compose
- Go 1.18 or higher (for local development)

### Building the Container
```bash
# Build the Docker image
docker build -t prj3 .
```

### Running Test Cases
The system includes four different test scenarios, each with its own docker-compose file:

```bash
# Test Case 1: Basic Join Operations
docker-compose -f docker-compose-testcase-1.yml up

# Test Case 2: Single Node Failure
docker-compose -f docker-compose-testcase-2.yml up

# Test Case 3: Multiple Node Failures
docker-compose -f docker-compose-testcase-3.yml up

# Test Case 4: Leader Failure with Message Loss
docker-compose -f docker-compose-testcase-4.yml up
```

### Command Line Arguments
The program accepts the following command-line flags:
- `-h string`: Path to the hosts file (required)
- `-d int`: Initial delay in seconds (default: 0)
- `-c int`: Crash delay in seconds
- `-t bool`: Trigger crash simulation

## System Architecture

### Network Communication
- TCP Port: 8080 (membership management)
- UDP Port: 8081 (heartbeat messages)
- Heartbeat Interval: 1.1 seconds
- Monitor Interval: 2.2 seconds

### Key Features
1. **Membership Management**
   - View-based consistency
   - Quorum-based acknowledgments
   - Member addition/removal

2. **Failure Detection**
   - Heartbeat-based monitoring
   - UDP broadcast for efficiency
   - Configurable monitoring intervals

3. **Leader Election**
   - Automatic leader election on failure
   - State synchronization after election
   - Pending operation recovery

## Implementation Notes

### Important Considerations
1. **Network Configuration**
   - Ensure ports 8080 and 8081 are available
   - Docker network must support UDP broadcast
   - Hostname resolution must be functional

2. **Safety Mechanisms**
   - Thread-safe data structures
   - Mutex-protected state changes
   - Atomic view updates

3. **Failure Handling**
   - Graceful handling of network partitions
   - Recovery from message loss
   - State consistency maintenance

### Error Conditions
1. **Network Errors**
   ```
   Error starting TCP listener: address already in use
   Error reading from UDP connection: connection refused
   Error getting hostname from address: no such host
   ```

2. **State Errors**
   ```
   Error: member ID not found in peerIdToName map
   Error encoding integer: invalid argument
   Error decoding byte array to integers: unexpected EOF
   ```

## Testing
The system includes four test scenarios:

1. **Test Case 1**: Basic membership protocol
   - Tests join operations
   - Verifies view synchronization
   - Validates message handling

2. **Test Case 2**: Single node failure
   - Tests failure detection
   - Verifies member removal
   - Validates state updates

3. **Test Case 3**: Multiple node failures
   - Tests multiple node failures
   - Verifies member removal
   - Validates state updates

4. **Test Case 4**: Leader failure with message loss
   - Tests leader failure handling
   - Verifies message loss recovery
   - Validates consistency maintenance
