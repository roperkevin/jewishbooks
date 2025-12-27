from __future__ import annotations

import json
from typing import Iterable, List

from isbn_harvester.core.models import BookRow


def _row_to_dict(r: BookRow) -> dict:
    return {
        "isbn13": r.isbn13,
        "title": r.title or r.title_long,
        "authors": r.authors,
        "publisher": r.publisher,
        "language": r.language,
        "subjects": r.subjects,
        "jewish_score": r.jewish_score,
        "rank_score": r.rank_score,
        "cover": r.cloudfront_cover_url or r.cover_url_original or r.cover_url,
    }


def write_dashboard(rows: Iterable[BookRow], out_path: str, *, max_rows: int = 500) -> None:
    rows = list(rows)
    rows.sort(key=lambda r: r.rank_score, reverse=True)
    payload: List[dict] = [_row_to_dict(r) for r in rows[:max_rows]]

    data_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>ISBN Harvester Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #111; }}
    .controls {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 12px; }}
    input {{ padding: 6px 10px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 10px; text-align: left; }}
    th {{ background: #f3f3f3; position: sticky; top: 0; }}
    img {{ max-height: 80px; }}
  </style>
</head>
<body>
  <h1>ISBN Harvester Dashboard</h1>
  <div class="controls">
    <label>Search <input id="q" placeholder="title, author, subject" /></label>
    <label>Min score <input id="minScore" type="number" value="0" /></label>
    <label>Min rank <input id="minRank" type="number" step="0.01" value="0" /></label>
  </div>
  <table>
    <thead>
      <tr>
        <th>Cover</th>
        <th>Title</th>
        <th>Authors</th>
        <th>Publisher</th>
        <th>Language</th>
        <th>Subjects</th>
        <th>Jewish Score</th>
        <th>Rank</th>
        <th>ISBN13</th>
      </tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>

  <script>
    const data = {data_json};
    const rowsEl = document.getElementById('rows');
    const qEl = document.getElementById('q');
    const minScoreEl = document.getElementById('minScore');
    const minRankEl = document.getElementById('minRank');

    function esc(value) {{
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    function render() {{
      const q = (qEl.value || '').toLowerCase();
      const minScore = parseFloat(minScoreEl.value || '0');
      const minRank = parseFloat(minRankEl.value || '0');
      rowsEl.innerHTML = '';

      const filtered = data.filter(r => {{
        if (r.jewish_score < minScore) return false;
        if (r.rank_score < minRank) return false;
        if (!q) return true;
        const hay = `${{r.title}} ${{r.authors}} ${{r.subjects}}`.toLowerCase();
        return hay.includes(q);
      }});

      for (const r of filtered) {{
        const tr = document.createElement('tr');
        const coverCell = r.cover ? `<img src="${{esc(r.cover)}}" />` : '';
        tr.innerHTML = `
          <td>${{coverCell}}</td>
          <td>${{esc(r.title)}}</td>
          <td>${{esc(r.authors)}}</td>
          <td>${{esc(r.publisher)}}</td>
          <td>${{esc(r.language)}}</td>
          <td>${{esc(r.subjects)}}</td>
          <td>${{r.jewish_score}}</td>
          <td>${{r.rank_score.toFixed(3)}}</td>
          <td>${{esc(r.isbn13)}}</td>
        `;
        rowsEl.appendChild(tr);
      }}
    }}

    qEl.addEventListener('input', render);
    minScoreEl.addEventListener('input', render);
    minRankEl.addEventListener('input', render);
    render();
  </script>
</body>
</html>
"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
