import io
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf
from monitor.logger import log

def create_chart(df_plot, symbol, timeframe):
    """
    Создаёт график свечей с индикаторами и возвращает его в виде байтов.
    """
    try:
        log(f"Колонки в df_plot для {symbol}: {list(df_plot.columns)}", level="debug")
        if len(df_plot) < 2:
            log(f"Недостаточно данных для построения графика {symbol}: {len(df_plot)} свечей", level="warning")
            return None

        df_plot = df_plot.copy()
        add_plots = []

        if all(col in df_plot for col in ['sma20', 'upper', 'lower']):
            add_plots.extend([
                mpf.make_addplot(df_plot['sma20'], color='orange', linestyle='--', width=1, ylabel='Price'),
                mpf.make_addplot(df_plot['upper'], color='purple', linestyle=':', width=0.8),
                mpf.make_addplot(df_plot['lower'], color='purple', linestyle=':', width=0.8)
            ])
        else:
            log(f"Bollinger columns (sma20, upper, lower) missing for {symbol}, skipping plot", level="warning")

        if 'rsi' in df_plot:
            add_plots.append(mpf.make_addplot(df_plot['rsi'], panel=1, color='blue', ylabel='RSI'))
        else:
            log(f"RSI column missing for {symbol}, skipping plot", level="warning")

        if all(col in df_plot for col in ['macd', 'signal', 'macd_hist']):
            add_plots.extend([
                mpf.make_addplot(df_plot['macd'], panel=2, color='blue', ylabel='MACD'),
                mpf.make_addplot(df_plot['signal'], panel=2, color='orange', linestyle='--'),
                mpf.make_addplot(df_plot['macd_hist'], type='bar', panel=2, color='gray', alpha=0.5)
            ])
        else:
            log(f"MACD columns (macd, signal, macd_hist) missing for {symbol}, skipping plot", level="warning")

        if 'adx' in df_plot:
            add_plots.append(mpf.make_addplot(df_plot['adx'], panel=3, color='green', ylabel='ADX'))
        else:
            log(f"ADX column missing for {symbol}, skipping plot", level="warning")

        if not add_plots:
            log(f"Нет индикаторов для отображения на графике для {symbol}", level="warning")
            return None

        fig, axes = mpf.plot(
            df_plot,
            type='candle',
            style='yahoo',
            title=f"{symbol} ({timeframe})",
            ylabel='Price (USDT)',
            addplot=add_plots,
            volume=True,
            panel_ratios=(3, 1, 1, 1) if add_plots else (3, 1),
            figsize=(12, 8),
            returnfig=True
        )

        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return buf

    except Exception as e:
        log(f"Ошибка создания графика для {symbol}: {str(e)}", level="error")
        return None
