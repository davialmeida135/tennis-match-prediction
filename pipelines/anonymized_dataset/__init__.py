"""Anonymized training dataset, published as a versioned W&B artifact.

Reads the dataset published by the `historical_matches` pipeline, hides which
player won, and publishes the result back to W&B for the ML step to consume.
"""
