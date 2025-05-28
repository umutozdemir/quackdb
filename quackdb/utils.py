import re
from typing import Optional, List, Tuple

# Regex to extract projection, parquet files, and predicate
_SELECT = re.compile(r"SELECT\s+(.*?)\s+FROM", re.IGNORECASE)
_FROM_PQ = re.compile(
    r"FROM\s+read_parquet\(\[([^\]]+)\]\)",
    re.IGNORECASE
)
_WHERE = re.compile(
    r"WHERE\s+([a-zA-Z_]\w*)\s*(=|>|<|>=|<=|!=)\s*([0-9.]+)",
    re.IGNORECASE
)

def parse_sql(sql: str) -> Optional[Tuple[List[str], Optional[List[str]], Optional[str], Optional[str], Optional[float]]]:
    """
    Returns (files, projection_columns, filter_column, operator, value)
    projection_columns is None if '*' or not found.
    """
    # projection
    m_sel = _SELECT.search(sql)
    proj = None
    if m_sel:
        cols = [c.strip().strip('"') for c in m_sel.group(1).split(',')]
        if cols != ['*']:
            proj = cols
    # files
    m_from = _FROM_PQ.search(sql)
    if not m_from:
        return None
    
    # Extract the content inside the brackets
    files_content = m_from.group(1)
    
    # Split by comma and clean up each file path
    files = []
    for file_part in files_content.split(','):
        # Strip whitespace and remove quotes if present
        file_path = file_part.strip().strip("'\"")
        files.append(file_path)
    # predicate
    m_wh = _WHERE.search(sql)
    if m_wh:
        col, op, val = m_wh.group(1), m_wh.group(2), float(m_wh.group(3))
    else:
        col = op = val = None
    return files, proj, col, op, val