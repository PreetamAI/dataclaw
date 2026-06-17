import { Loader2, Save, X } from "lucide-react";
import { useEffect, useState } from "react";

import { useCreateEvalCaseFromFeedbackMutation } from "../services/api";
import type { ChatCitation, ChatMessage } from "../types";

type Status = "idle" | "submitting" | "saved" | "error";

/**
 * Structured correction form rendered after a 👎. Saves to /evals/cases/from-feedback
 * as status="candidate" — the user (or another reviewer) still has to approve
 * it before it counts as an eval case, and a separate explicit click to promote
 * to golden.
 */
export function EvalCorrectionForm({
  message,
  onClose,
}: {
  message: ChatMessage;
  onClose: () => void;
}) {
  const [submit, submitState] = useCreateEvalCaseFromFeedbackMutation();
  const [question, setQuestion] = useState<string>("");
  const [expectedAnswer, setExpectedAnswer] = useState<string>("");
  const [expectedSql, setExpectedSql] = useState<string>(message.sql ?? "");
  const [expectedConnector, setExpectedConnector] = useState<string>("");
  const [expectedTool, setExpectedTool] = useState<string>("");
  const [pickedCitations, setPickedCitations] = useState<ChatCitation[]>([]);
  const [status, setStatus] = useState<Status>("idle");

  useEffect(() => {
    function onKey(event: KeyboardEvent) {
      if (event.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const toggleCitation = (citation: ChatCitation) => {
    setPickedCitations((prev) =>
      prev.includes(citation)
        ? prev.filter((c) => c !== citation)
        : [...prev, citation],
    );
  };

  const onSave = async () => {
    if (status === "submitting") return;
    const hasExpected = expectedAnswer.trim() || expectedSql.trim() || expectedTool.trim();
    if (!hasExpected) {
      setStatus("error");
      return;
    }
    setStatus("submitting");
    try {
      await submit({
        chat_message_id: message.id,
        question: question.trim() || null,
        expected_answer: expectedAnswer.trim() || null,
        expected_sql: expectedSql.trim() || null,
        expected_connector_slug: expectedConnector.trim() || null,
        expected_tool: expectedTool.trim() || null,
        expected_citations: pickedCitations.length
          ? pickedCitations.map((c) => ({
              source: c.connector,
              table: c.title,
              type: "citation",
            }))
          : null,
      }).unwrap();
      setStatus("saved");
    } catch {
      setStatus("error");
    }
  };

  const submitting = status === "submitting" || submitState.isLoading;

  return (
    <div className="modal-backdrop" onClick={onClose} role="presentation">
      <div
        className="modal-card"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="Save correction as eval case"
      >
        <header>
          <div>
            <strong>Save correction as eval case</strong>
            <em>
              Lands as a candidate. Review it under Evals to approve, then promote to a
              golden query to short-circuit future answers.
            </em>
          </div>
          <button aria-label="Close" className="ghost icon-button" onClick={onClose} type="button">
            <X size={16} />
          </button>
        </header>

        <div className="modal-body">
          <div className="modal-fields">
            <label>
              <span>Question (leave blank to use the original)</span>
              <input
                type="text"
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                placeholder="e.g. how many active customers per region last month?"
              />
            </label>

            <label>
              <span>Expected answer</span>
              <textarea
                rows={3}
                value={expectedAnswer}
                onChange={(e) => setExpectedAnswer(e.target.value)}
                placeholder="What the assistant should have said."
              />
            </label>

            <label>
              <span>Expected SQL</span>
              <textarea
                className="eval-correction-sql"
                rows={4}
                value={expectedSql}
                onChange={(e) => setExpectedSql(e.target.value)}
                placeholder="SELECT count(*) FROM core.customers WHERE ..."
              />
            </label>

            <div className="eval-correction-row">
              <label>
                <span>Expected connector</span>
                <input
                  type="text"
                  value={expectedConnector}
                  onChange={(e) => setExpectedConnector(e.target.value)}
                  placeholder="e.g. postgres"
                />
              </label>

              <label>
                <span>Expected tool</span>
                <input
                  type="text"
                  value={expectedTool}
                  onChange={(e) => setExpectedTool(e.target.value)}
                  placeholder="e.g. postgres.read_select"
                />
              </label>
            </div>

            {message.citations.length > 0 ? (
              <div className="eval-correction-citations">
                <span>Pick required citations</span>
                {message.citations.map((c, index) => (
                  <label key={`${c.title}-${index}`} className="eval-correction-citation">
                    <input
                      type="checkbox"
                      checked={pickedCitations.includes(c)}
                      onChange={() => toggleCitation(c)}
                    />
                    {c.title} <em>({c.connector})</em>
                  </label>
                ))}
              </div>
            ) : null}
          </div>

          {status === "saved" ? (
            <div className="modal-result ok">
              <strong>Saved</strong>
              <p>Review it under Evals to approve.</p>
            </div>
          ) : null}
          {status === "error" ? (
            <div className="modal-result failed">
              <strong>Missing expected value</strong>
              <p>Fill at least one of Expected answer, Expected SQL, or Expected tool.</p>
            </div>
          ) : null}
        </div>

        <footer>
          <button className="ghost" onClick={onClose} type="button">
            {status === "saved" ? "Close" : "Cancel"}
          </button>
          {status !== "saved" ? (
            <button className="primary" disabled={submitting} onClick={onSave} type="button">
              {submitting ? <Loader2 className="spin" size={14} /> : <Save size={14} />}
              Save as candidate eval
            </button>
          ) : null}
        </footer>
      </div>
    </div>
  );
}
