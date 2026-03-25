import sqlite3
import pandas as pd

def query_db():
    conn = sqlite3.connect('data/keiba.db')
    
    # 2026-03-08 中山 11R
    q1 = """
    SELECT rs.rank, rs.horse_number, h.name 
    FROM results rs 
    JOIN races r ON r.id = rs.race_id 
    JOIN horses h ON h.id = rs.horse_id 
    WHERE r.date = '2026-03-08' AND r.place = '中山' AND r.race_number = 11 
    ORDER BY rs.rank LIMIT 5;
    """
    print("--- 2026-03-08 中山11R ---")
    print(pd.read_sql_query(q1, conn))
    
    # 2026-03-08 阪神のレース結果
    q2 = """
    SELECT r.race_number, rs.rank, rs.horse_number, h.name 
    FROM results rs 
    JOIN races r ON r.id = rs.race_id 
    JOIN horses h ON h.id = rs.horse_id 
    WHERE r.date = '2026-03-08' AND r.place = '阪神' AND rs.rank <= 3
    ORDER BY r.race_number, rs.rank;
    """
    print("--- 2026-03-08 阪神 1~12R (3着以内) ---")
    print(pd.read_sql_query(q2, conn))
    
    conn.close()

if __name__ == "__main__":
    query_db()
