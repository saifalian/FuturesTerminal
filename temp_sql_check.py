import sqlite3
p=r"D:\PROJECTS\20 sec\futures-terminal\backend\data\sqlite\terminal.db"
conn=sqlite3.connect(p)
cur=conn.cursor()
queries=[
("select count(*), min(ts_receive_ms), max(ts_receive_ms) from raw_events where pair_symbol='XRPUSDT'",'raw_xrp'),
("select count(*), min(ts_start_ms), max(ts_end_ms), sum(row_count) from market_event_chunks where pair_symbol='XRPUSDT'",'chunks_xrp'),
("select count(*) from raw_events",'raw_all'),
("select count(*) from market_event_chunks",'chunks_all')
]
for q,name in queries:
    try:
        print(name, cur.execute(q).fetchone())
    except Exception as e:
        print(name,'ERR',e)
try:
    print('raw by capture', cur.execute("select capture_mode,count(*) from raw_events where pair_symbol='XRPUSDT' group by 1").fetchall())
except Exception as e:
    print('raw by capture err',e)
conn.close()
