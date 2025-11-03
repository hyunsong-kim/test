import datetime, decimal

def fetch_table_columns_and_types(conn, owner, name):
    sql = """
    select column_name, data_type
      from all_tab_columns
     where owner=:o and table_name=:t
       and nvl(identity_column,'NO')='NO'
       and nvl(virtual_column,'NO')='NO'
     order by column_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, o=owner.upper(), t=name.upper())
        cols = []
        types = {}
        for c, dt in cur.fetchall():
            c = c.upper(); dt = dt.upper()
            cols.append(c); types[c] = dt
        return cols, types

def coerce_row_types(row: dict, types: dict) -> dict:
    """row는 {COL: value} (COL은 UPPER), types는 {COL: 'DATE'/'TIMESTAMP'/'NUMBER'...}"""
    out = dict(row)
    for c, dt in types.items():
        v = out.get(c)
        if v is None: 
            continue
        # 문자열일 때만 변환 시도
        if isinstance(v, str):
            s = v.strip()
            if dt == 'DATE':
                # 허용 포맷: 'YYYY-MM-DD' 또는 'YYYY-MM-DDTHH:MM:SS'
                if 'T' in s:
                    s = s.replace('T', ' ')[:19]
                out[c] = datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S') if ' ' in s \
                         else datetime.datetime.strptime(s, '%Y-%m-%d')
            elif dt.startswith('TIMESTAMP WITH TIME ZONE'):
                # ISO 'Z' 처리
                if s.endswith('Z'): s = s[:-1] + '+00:00'
                # Python 3.11+: fromisoformat 지원, 이전엔 아래 치환 필수
                out[c] = datetime.datetime.fromisoformat(s)
            elif dt.startswith('TIMESTAMP'):
                # 마이크로초 포함 가능
                if s.endswith('Z'): s = s[:-1] + '+00:00'  # 혹시 들어오면 tz 제거/치환
                # 'YYYY-MM-DDTHH:MM:SS[.ffffff]' 지원
                s2 = s.replace('T',' ')
                try:
                    out[c] = datetime.datetime.fromisoformat(s2)
                except ValueError:
                    out[c] = datetime.datetime.strptime(s2[:19], '%Y-%m-%d %H:%M:%S')
            elif dt in ('NUMBER','FLOAT','BINARY_FLOAT','BINARY_DOUBLE'):
                try:
                    out[c] = decimal.Decimal(s)
                except Exception:
                    pass
        # 이미 datetime이면 그대로 사용
    return out
