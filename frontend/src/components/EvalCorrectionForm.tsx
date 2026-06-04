import { useState } from "react";

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

  return (
    <div className="eval-correction-form">
      <h4>Save correction as eval case</h4>
      <p className="eval-correction-hint">
        Lands as a <strong>candidate</strong>. Review it under Evals to approve,
        then promote to a golden query to short-circuit future answers.
      </p>

      <label className="settings-field">
        <span>Question (leave blank to use the original)</span>
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
          placeholder="e.g. how many active customers per region last month?"
        />
      </label>

      <label className="settings-field">
        <span>Expected answer</span>
        <textarea
          rows={3}
          value={expectedAnswer}
          onChange={(e) => setExpectedAnswer(e.target.value)}
          placeholder="What the assistant should have said."
        />
      </label>

      <label className="settings-field">
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
        <label className="settings-field">
          <span>Expected connector</span>
          <input
            type="text"
            value={expectedConnector}
            onChange={(e) => setExpectedConnector(e.target.value)}
            placeholder="e.g. postgres"
          />
        </label>

        <label className="settings-field">
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

      <div className="eval-correction-actions">
        <button
          type="button"
          onClick={onSave}
          disabled={status === "submitting" || submitState.isLoading}
        >
          Save as candidate eval
        </button>
        <button type="button" className="feedback-link" onClick={onClose}>
          Cancel
        </button>
        {status === "saved" ? (
          <span className="feedback-status feedback-status-ok">
            Saved. Review under Evals.
          </span>
        ) : null}
        {status === "error" ? (
          <span className="feedback-status feedback-status-error">
            Fill at least one expected_* field.
          </span>
        ) : null}
      </div>
    </div>
  );
}
