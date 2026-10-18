"""python -m relay.server [--port 8080] [--dsn ...]"""

import argparse
import logging

import uvicorn

from relay.server.app import create_app


def main() -> None:
    p = argparse.ArgumentParser(prog="relay.server")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--dsn", default=None)
    p.add_argument("--log-level", default="warning")
    args = p.parse_args()
    logging.basicConfig(level=args.log_level.upper())
    uvicorn.run(
        create_app(args.dsn),
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        access_log=False,
    )


if __name__ == "__main__":
    main()
