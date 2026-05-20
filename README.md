# Basic Microservice Architecture

## Requirements
- Docker Desktop
- Python 3.x + `pip install httpx`

## Run

```bash
docker compose up --build
```

## API

| Method | URL | Body |
|---|---|---|
| POST | `http://localhost:8000/transaction` | `{"user_id": "alice", "amount": 100}` |
| GET | `http://localhost:8000/user/{user_id}` | — |
| GET | `http://localhost:8000/accounts` | — |
| GET | `http://localhost:8000/stats` | — |

## Test

```bash
# functional demo
python client/client.py --user alice --amount 50 --n 3

# performance scenarios (restart containers first for clean state)
python client/tests.py --scenario 1
python client/tests.py --scenario 2
```