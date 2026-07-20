"""Document Graph Config — minimal wrapper."""
class DocumentGraphConfig:
    """Minimal config. AWS operations go through graphrag-toolkit."""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
