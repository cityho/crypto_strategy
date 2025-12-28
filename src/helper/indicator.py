import pandas as pd
import numpy as np


def rsi(close: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    """Wilder-style RSI (vectorized)."""
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)

    # Wilder smoothing via EMA(alpha=1/window)
    roll_up = up.ewm(alpha=1/window, adjust=False).mean()
    roll_down = down.ewm(alpha=1/window, adjust=False).mean()

    rs = roll_up / roll_down.replace(0.0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    return out.fillna(0.0)