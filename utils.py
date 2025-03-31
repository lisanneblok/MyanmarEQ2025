import yaml
from pathlib import Path

def read_yaml(fp: str | Path) -> dict:
    with open(fp, 'r') as f:
        config = yaml.safe_load(f)
        
    return config
        
        
def specify_utc(dt):
    """
    Specify UTC timezone for a datetime object
    """
    import pytz
    from datetime import datetime

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    else:
        dt = dt.astimezone(pytz.UTC)
        
    return dt