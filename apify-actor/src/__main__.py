"""Allows the actor to be started with `python -m src`."""

import asyncio

from .main import main

asyncio.run(main())
