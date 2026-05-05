# Топ-30 ликвидных акций Московской биржи
TICKER_LIST = [
    'SBER', 'GAZP', 'MOEX', 'YDEX', 'LKOH', 'ROSN', 'NVTK', 'MGNT', 'TCSG', 'PLZL',
    'CHMF', 'MTSS', 'ALRS', 'VTBR', 'TATN', 'GMKN', 'NLMK', 'IRAO', 'RTKM', 'AFLT',
    'PIKK', 'MAGN', 'CBOM', 'VKCO', 'TRNFP', 'PHOR', 'AFKS', 'ENPG', 'UPRO', 'HYDR'
]

# interval = 1 (Минутные свечи, чтобы сгенерировать объем данных)
TICKERS = [{"ticker": t, "interval": 1} for t in TICKER_LIST]