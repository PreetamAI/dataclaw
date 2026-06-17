import type { ReactNode } from "react";

export type TocItem = { text: string; slug: string; level: number };

export function slugify(text: string): string {
  return text
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-");
}

export function childText(children: ReactNode): string {
  if (children == null || children === false) return "";
  if (typeof children === "string" || typeof children === "number") return String(children);
  if (Array.isArray(children)) return children.map(childText).join("");
  if (typeof children === "object" && "props" in children) {
    return childText((children as { props: { children?: ReactNode } }).props.children);
  }
  return "";
}

export function extractHeadings(body: string): TocItem[] {
  const items: TocItem[] = [];
  let inFence = false;
  for (const raw of body.split("\n")) {
    if (/^\s*```/.test(raw)) {
      inFence = !inFence;
      continue;
    }
    if (inFence) continue;
    const match = /^(#{2,3})\s+(.+?)\s*#*$/.exec(raw);
    if (!match) continue;
    const text = match[2].replace(/[*_`]/g, "").trim();
    if (!text) continue;
    items.push({ text, slug: slugify(text), level: match[1].length });
  }
  return items;
}
