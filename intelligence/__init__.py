"""PSYGRID intelligence layer.

Reads only what PSYGRID already writes (today: the daily archive) and never
calls Dhan or imports a live manager, so it cannot affect the feeds. Every
engine is a function of a ``MarketFrame``: the market exactly as it was known at
one minute, with its data quality, never anything later.
"""
