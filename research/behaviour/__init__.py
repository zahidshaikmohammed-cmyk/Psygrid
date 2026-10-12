"""Second-generation stock behaviour discovery.

Pipeline: data (stream -> day matrices) -> features (causal states, events, outcomes) ->
engine (hypotheses, discovery/validation/holdout, walk-forward, fingerprints) -> run (outputs).
"""
