import { useState } from "react";

import {
  useChatTraceLinkQuery,
  useSubmitFeedbackMutation,
} from "../services/api";
import type { ChatMessage } from "../types";

type Status = "idle" | "submitting" | "submitted" | "error";

export function FeedbackBar({ message }: { message: ChatMessage }) {
  const messageId = message.id;
  const [submit, submitState] = useSubmitFeedbackMutation();
  const [selected, setSelected] = useState<"positive" | "negative" | null>(null);
  const [commentOpen, setCommentOpen] = useState(false);
  const [comment, setComment] = useState("");
  const [status, setStatus] = useState<Status>("idle");

  // Lazy: only fetch the Langfuse link when the user expands "view trace".
  const [traceOpen, setTraceOpen] = useState(false);
  const { data: traceLink } = useChatTraceLinkQuery(messageId, {
    skip: !traceOpen,
  });

  const onReact = async (sentiment: "positive" | "negative") => {
    if (status === "submitting") return;
    setSelected(sentiment);
    setStatus("submitting");
    try {
      await submit({
        chat_message_id: messageId,
        sentiment,
      }).unwrap();
      setStatus("submitted");
      if (sentiment === "negative") {
        setCommentOpen(true);
      }
    } catch {
      setStatus("error");
    }
  };

  const saveComment = async () => {
    if (!comment.trim()) {
      setCommentOpen(false);
      return;
    }
    setStatus("submitting");
    try {
      await submit({
        chat_message_id: messageId,
        sentiment: "negative",
        comment: comment.trim(),
      }).unwrap();
      setStatus("submitted");
      setCommentOpen(false);
      setComment("");
    } catch {
      setStatus("error");
    }
  };

  return (
    <div className="feedback-bar">
      <div className="feedback-bar-row">
        <button
          type="button"
          className={`feedback-button ${selected === "positive" ? "feedback-button-selected" : ""}`}
          onClick={() => onReact("positive")}
          disabled={submitState.isLoading}
          aria-label="Mark answer as helpful"
        >
          {selected === "positive" ? "👍 Thanks" : "👍"}
        </button>
        <button
          type="button"
          className={`feedback-button ${selected === "negative" ? "feedback-button-selected" : ""}`}
          onClick={() => onReact("negative")}
          disabled={submitState.isLoading}
          aria-label="Mark answer as wrong"
        >
          {selected === "negative" ? "👎 Noted" : "👎"}
        </button>
        <button
          type="button"
          className="feedback-link"
          onClick={() => setTraceOpen((open) => !open)}
        >
          {traceOpen ? "Hide trace" : "View trace"}
        </button>
        {status === "submitted" && !commentOpen ? (
          <span className="feedback-status feedback-status-ok">Saved</span>
        ) : null}
        {status === "error" ? (
          <span className="feedback-status feedback-status-error">
            Could not save feedback
          </span>
        ) : null}
      </div>

      {commentOpen && selected === "negative" ? (
        <div className="feedback-comment">
          <label htmlFor={`feedback-comment-${messageId}`}>
            Tell us what went wrong (optional)
          </label>
          <textarea
            id={`feedback-comment-${messageId}`}
            value={comment}
            onChange={(e) => setComment(e.target.value)}
            placeholder="e.g. wrong table, wrong join, hallucinated column…"
            rows={3}
          />
          <div className="feedback-comment-actions">
            <button
              type="button"
              onClick={() => {
                setCommentOpen(false);
                setComment("");
              }}
            >
              Skip
            </button>
            <button type="button" onClick={saveComment}>
              Save comment
            </button>
          </div>
        </div>
      ) : null}

      {traceOpen ? (
        <div className="feedback-trace">
          {traceLink?.langfuse_url ? (
            <a
              href={traceLink.langfuse_url}
              target="_blank"
              rel="noreferrer"
              className="feedback-link"
            >
              Open in Langfuse →
            </a>
          ) : traceLink ? (
            <span className="feedback-trace-note">
              Local trace recorded
              {traceLink.trace_id ? ` (id ${traceLink.trace_id.slice(0, 8)}…)` : ""}.
              Configure Langfuse under Settings → Integrations for a richer view.
            </span>
          ) : (
            <span className="feedback-trace-note">Loading trace…</span>
          )}
        </div>
      ) : null}
    </div>
  );
}
