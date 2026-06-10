import { useMemo } from "react";

const KEYWORDS = new Set([
  "select", "from", "where", "group", "by", "order", "having", "limit",
  "offset", "join", "inner", "left", "right", "outer", "full", "on",
  "as", "and", "or", "not", "in", "is", "null", "case", "when", "then",
  "else", "end", "with", "distinct", "union", "all", "intersect", "except",
  "insert", "into", "values", "update", "set", "delete", "create", "table",
  "drop", "alter", "view", "index", "exists", "like", "between", "asc",
  "desc", "interval", "count", "sum", "avg", "max", "min", "now",
]);

type Token = { value: string; cls?: string };

function tokenize(sql: string): Token[] {
  const out: Token[] = [];
  let i = 0;
  while (i < sql.length) {
    const ch = sql[i];
    if (ch === "-" && sql[i + 1] === "-") {
      const end = sql.indexOf("\n", i);
      const slice = end === -1 ? sql.slice(i) : sql.slice(i, end);
      out.push({ value: slice, cls: "sql-comment" });
      i += slice.length;
      continue;
    }
    if (ch === "'" || ch === '"') {
      let j = i + 1;
      while (j < sql.length && sql[j] !== ch) {
        if (sql[j] === "\\") j += 2;
        else j += 1;
      }
      out.push({ value: sql.slice(i, j + 1), cls: "sql-string" });
      i = j + 1;
      continue;
    }
    if (/\s/.test(ch)) {
      let j = i;
      while (j < sql.length && /\s/.test(sql[j])) j += 1;
      out.push({ value: sql.slice(i, j) });
      i = j;
      continue;
    }
    if (/[0-9]/.test(ch)) {
      let j = i;
      while (j < sql.length && /[0-9.]/.test(sql[j])) j += 1;
      out.push({ value: sql.slice(i, j), cls: "sql-number" });
      i = j;
      continue;
    }
    if (/[a-zA-Z_]/.test(ch)) {
      let j = i;
      while (j < sql.length && /[a-zA-Z0-9_.]/.test(sql[j])) j += 1;
      const word = sql.slice(i, j);
      const cls = KEYWORDS.has(word.toLowerCase()) ? "sql-keyword" : undefined;
      out.push({ value: word, cls });
      i = j;
      continue;
    }
    out.push({ value: ch, cls: "sql-punct" });
    i += 1;
  }
  return out;
}

export function SqlBlock({ sql, label = "SQL" }: { sql: string; label?: string }) {
  const tokens = useMemo(() => tokenize(sql), [sql]);
  return (
    <div className="sql-block">
      <span className="sql-block-label">{label}</span>
      <pre>
        <code>
          {tokens.map((token, index) =>
            token.cls ? (
              <span key={index} className={token.cls}>{token.value}</span>
            ) : (
              token.value
            ),
          )}
        </code>
      </pre>
    </div>
  );
}
