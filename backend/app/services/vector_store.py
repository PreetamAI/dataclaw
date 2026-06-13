from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
from chromadb.errors import NotFoundError
from openai import OpenAI

from app.core.config import get_settings
from app.models.domain import (
    AgentRun,
    Alert,
    KnowledgeDocument,
    KnowledgeNode,
    LineageEdge,
    TableAsset,
    WikiPage,
)
from app.services.ingestion.chunker import Chunk

logger = logging.getLogger("dataclaw.vector_store")


class ChromaUnreachableError(RuntimeError):
    pass


@dataclass
class VectorResult:
    id: str
    document: str
    metadata: dict[str, Any]
    distance: float | None = None

    @property
    def asset_type(self) -> str:
        return str(self.metadata.get("asset_type", ""))


class OpenAICompatibleEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(self, *, api_key: str, base_url: str | None, model: str) -> None:
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model = model

    def __call__(self, input: Documents) -> Embeddings:  # noqa: A002 - Chroma's protocol uses input.
        response = self.client.embeddings.create(model=self.model, input=list(input))
        return [item.embedding for item in sorted(response.data, key=lambda item: item.index)]

    def name(self) -> str:
        return f"openai-compatible:{self.model}"

    def get_config(self) -> dict[str, Any]:
        return {"model": self.model, "base_url": str(self.client.base_url) if self.client.base_url else None}

    @staticmethod
    def build_from_config(config: dict[str, Any]) -> OpenAICompatibleEmbeddingFunction:
        raise NotImplementedError("Rebuild via VectorStore.ensure_embedding_model() — credentials live in the LLM provider store.")


class VectorStore:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._client = None
        self._collections: dict[str, Any] = {}
        self._test_double: dict[str, dict[str, tuple[str, dict[str, Any]]]] = {}
        self._embedding_function = None
        self._embedding_api_key: str | None = None
        self._embedding_base_url: str | None = None

    def _chroma_location(self) -> str:
        return self.settings.chroma_url or self.settings.chroma_path

    def _get_client(self):
        client = self._client
        if client is not None:
            return client
        import chromadb

        if self.settings.chroma_url:
            host_port = self.settings.chroma_url.replace("http://", "").replace("https://", "")
            host, _, port = host_port.partition(":")
            client = chromadb.HttpClient(host=host, port=int(port or "8000"))
        else:
            Path(self.settings.chroma_path).mkdir(parents=True, exist_ok=True)
            client = chromadb.PersistentClient(path=self.settings.chroma_path)
        self._client = client
        return client

    def _collection_name(self, workspace_id: str) -> str:
        return f"ws_{workspace_id.replace('-', '_')}"

    def _brain_collection_name(self, workspace_id: str) -> str:
        return f"brain_summaries_{workspace_id.replace('-', '_')}"

    def active_embedding_model(self) -> str:
        return self.settings.embedding_model

    def _use_test_double(self) -> bool:
        return os.getenv("DATACLAW_VECTOR_TEST_DOUBLE", "").lower() in {"1", "true", "yes"}

    def _get_embedding_function(self):
        if self._embedding_function is not None:
            return self._embedding_function
        if self._embedding_api_key:
            self._embedding_function = OpenAICompatibleEmbeddingFunction(
                api_key=self._embedding_api_key,
                base_url=self._embedding_base_url,
                model=self.settings.embedding_model,
            )
            return self._embedding_function
        local_model = self.settings.embedding_model
        if self.settings.embedding_provider != "local" or local_model.startswith("text-embedding-"):
            local_model = "all-MiniLM-L6-v2"
            logger.warning(
                "embedding_fallback_local",
                extra={
                    "_configured_provider": self.settings.embedding_provider,
                    "_configured_model": self.settings.embedding_model,
                    "_fallback_model": local_model,
                    "_reason": "no OpenAI-compatible credentials available; using local SentenceTransformer",
                },
            )
            self.settings.embedding_model = local_model
        try:
            from chromadb.utils import embedding_functions
        except ImportError as exc:
            raise RuntimeError(
                "Local embedding fallback requires chromadb.utils — install chromadb with sentence-transformers extras."
            ) from exc
        self._embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=local_model,
        )
        return self._embedding_function

    def _get_collection(self, workspace_id: str, *, collection_name: str | None = None):
        if self._use_test_double():
            return None
        name = collection_name or self._collection_name(workspace_id)
        if name in self._collections:
            return self._collections[name]
        try:
            client = self._get_client()
            metadata = {"embedding_model": self.active_embedding_model()}
            collection = client.get_or_create_collection(
                name=name,
                embedding_function=self._get_embedding_function(),
                metadata=metadata,
            )
            collection_metadata = getattr(collection, "metadata", {}) or {}
            if collection_metadata.get("embedding_model") not in {None, self.active_embedding_model()}:
                client.delete_collection(name)
                collection = client.get_or_create_collection(
                    name=name,
                    embedding_function=self._get_embedding_function(),
                    metadata=metadata,
                )
            self._collections[name] = collection
            return collection
        except (ConnectionError, TimeoutError) as exc:
            raise ChromaUnreachableError(
                f"ChromaDB not reachable at {self._chroma_location()}. "
                "For Docker, start Chroma or set CHROMA_URL. For local installs, check CHROMA_PATH."
            ) from exc
        except Exception as exc:
            logger.exception(
                "vector_store_collection_error",
                extra={"_collection": name, "_error": exc.__class__.__name__},
            )
            raise RuntimeError(
                f"ChromaDB collection '{name}' failed: {exc.__class__.__name__}: {exc}"
            ) from exc

    def _collection_for(self, workspace_id: str, collection_name: str | None = None):
        if collection_name is None:
            return self._get_collection(workspace_id)
        try:
            return self._get_collection(workspace_id, collection_name=collection_name)
        except TypeError:
            if self._use_test_double():
                return None
            raise

    async def ping(self) -> None:
        if self._use_test_double():
            return
        try:
            client = self._get_client()
            client.heartbeat()
        except Exception as exc:
            raise ChromaUnreachableError(
                f"ChromaDB not reachable at {self._chroma_location()}. "
                "For Docker, start Chroma or set CHROMA_URL. For local installs, check CHROMA_PATH."
            ) from exc

    def _stable_id(self, *parts: Any) -> str:
        raw = "::".join(str(part) for part in parts)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    async def upsert_dataset(self, workspace_id: str, source_type: str, connector_slug: str, tables: list[TableAsset]) -> None:
        docs: list[tuple[str, str, dict[str, Any]]] = []
        for table in tables:
            table_id = self._stable_id("table", table.id)
            table_doc = f"{table.name}: {table.description or table.business_summary or 'table'}"
            docs.append(
                (
                    table_id,
                    table_doc,
                    {
                        "asset_type": "table",
                        "asset_id": table.id,
                        "connector_slug": connector_slug,
                        "source_type": source_type,
                        "schema_name": table.dataset.schema_name if table.dataset else "",
                        "table_name": table.name,
                        "related_tables": [table.name],
                    },
                )
            )
            for column in table.columns or []:
                name = column.get("name")
                if not name:
                    continue
                docs.append(
                    (
                        self._stable_id("column", table.id, name),
                        f"{table.name}.{name}: {column.get('type', '')} {column.get('description', '')}",
                        {
                            "asset_type": "column",
                            "asset_id": table.id,
                            "connector_slug": connector_slug,
                            "source_type": source_type,
                            "schema_name": table.dataset.schema_name if table.dataset else "",
                            "table_name": table.name,
                            "column_name": name,
                            "related_tables": [table.name],
                        },
                    )
                )
        await self._upsert(workspace_id, docs)

    def ids_for_table(self, table: TableAsset) -> list[str]:
        ids = [self._stable_id("table", table.id)]
        ids.extend(
            self._stable_id("column", table.id, column.get("name"))
            for column in (table.columns or [])
            if isinstance(column, dict) and column.get("name")
        )
        return ids

    async def upsert_knowledge_documents(self, workspace_id: str, docs: list[KnowledgeDocument]) -> None:
        await self._upsert(
            workspace_id,
            [
                (
                    self._stable_id("knowledge_document", doc.id),
                    f"{doc.title}\n{doc.body}",
                    {
                        "asset_type": "knowledge_document",
                        "asset_id": doc.id,
                        "connector_slug": doc.connector_slug,
                        "source_type": "knowledge_base",
                        "related_tables": doc.related_tables,
                        "title": doc.title,
                    },
                )
                for doc in docs
            ],
        )

    def id_for_wiki_page(self, page: WikiPage) -> str:
        return self._stable_id("wiki_page", page.workspace_id, page.path)

    async def upsert_wiki_pages(self, workspace_id: str, pages: list[WikiPage]) -> None:
        await self._upsert(
            workspace_id,
            [
                (
                    self.id_for_wiki_page(page),
                    f"{page.title}\n{page.body}",
                    {
                        "asset_type": "wiki_page",
                        "asset_id": page.id,
                        "path": page.path,
                        "disk_path": page.disk_path,
                        "connector_slug": page.source_type,
                        "source_type": page.source_type,
                        "source_id": page.source_id,
                        "tier": page.tier,
                        "title": page.title,
                        "entities": page.entities,
                        "related_tables": page.entities,
                    },
                )
                for page in pages
            ],
        )

    async def upsert_raw_chunks(
        self,
        workspace_id: str,
        source_type: str,
        source_id: str,
        chunks: list[Chunk],
    ) -> None:
        await self._upsert(
            workspace_id,
            [
                (
                    self._stable_id("raw_chunk", source_type, source_id, chunk.index),
                    chunk.content,
                    {
                        "asset_type": "raw_chunk",
                        "source_type": source_type,
                        "source_id": source_id,
                        "connector_slug": source_type,
                        "chunk_index": chunk.index,
                        "chunk_total": chunk.total,
                        **chunk.metadata,
                    },
                )
                for chunk in chunks
            ],
        )

    async def upsert_lineage_edges(self, workspace_id: str, edges: list[LineageEdge]) -> None:
        await self._upsert(
            workspace_id,
            [
                (
                    self._stable_id("lineage_edge", edge.id),
                    f"{edge.source_table} -> {edge.target_table}: {edge.relationship}. {edge.evidence}",
                    {
                        "asset_type": "lineage_edge",
                        "asset_id": edge.id,
                        "connector_slug": "lineage",
                        "source_type": "lineage",
                        "related_tables": [edge.source_table, edge.target_table],
                    },
                )
                for edge in edges
            ],
        )

    async def upsert_agent_runs(self, workspace_id: str, runs: list[AgentRun], alerts: list[Alert] | None = None) -> None:
        docs = [
            (
                self._stable_id("agent_run", run.id),
                f"{run.agent_name} {run.status}: {run.summary}",
                {
                    "asset_type": "agent_run",
                    "asset_id": run.id,
                    "connector_slug": run.agent_name,
                    "source_type": "agent_run",
                    "related_tables": [],
                },
            )
            for run in runs
        ]
        for alert in alerts or []:
            docs.append(
                (
                    self._stable_id("alert", alert.id),
                    f"{alert.severity}: {alert.title}. {alert.detail}",
                    {
                        "asset_type": "agent_run",
                        "asset_id": alert.id,
                        "connector_slug": "alerts",
                        "source_type": "alert",
                        "related_tables": [],
                    },
                )
            )
        await self._upsert(workspace_id, docs)

    async def upsert_brain_summaries(self, workspace_id: str, nodes: list[KnowledgeNode]) -> None:
        await self._upsert(
            workspace_id,
            [
                (
                    self._stable_id("brain_summary", node.id),
                    node.summary or f"{node.type} {node.canonical_name}",
                    {
                        "asset_type": "brain_summary",
                        "node_id": node.id,
                        "type": node.type,
                        "canonical_name": node.canonical_name,
                        "connector_slug": node.connector_slug,
                        "source_type": node.source_type,
                        "aliases": node.aliases,
                    },
                )
                for node in nodes
                if node.summary
            ],
            collection_name=self._brain_collection_name(workspace_id),
        )

    async def replace_brain_summaries(self, workspace_id: str, nodes: list[KnowledgeNode]) -> None:
        name = self._brain_collection_name(workspace_id)
        if self._use_test_double():
            self._test_double[name] = {}
            await self.upsert_brain_summaries(workspace_id, nodes)
            return
        new_ids = {self._stable_id("brain_summary", node.id) for node in nodes if node.summary}
        collection = self._collection_for(workspace_id, name)
        if collection is not None:
            existing_ids = set(collection.get(include=[]).get("ids") or [])
            stale_ids = list(existing_ids - new_ids)
            if stale_ids:
                collection.delete(ids=stale_ids)
        await self.upsert_brain_summaries(workspace_id, nodes)

    async def _upsert(
        self,
        workspace_id: str,
        docs: list[tuple[str, str, dict[str, Any]]],
        *,
        collection_name: str | None = None,
    ) -> None:
        if not docs:
            return
        docs = [(item_id, doc, self._clean_metadata(metadata)) for item_id, doc, metadata in docs]
        collection = self._collection_for(workspace_id, collection_name)
        if collection is None and self._use_test_double():
            bucket = self._test_double.setdefault(collection_name or self._collection_name(workspace_id), {})
            for item_id, doc, metadata in docs:
                bucket[item_id] = (doc, metadata)
            return
        batch_size = 500
        for index in range(0, len(docs), batch_size):
            batch = docs[index : index + batch_size]
            ids = [item[0] for item in batch]
            documents = [item[1] for item in batch]
            metadatas = [item[2] for item in batch]
            try:
                collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
            except NotFoundError:
                # The server-side collection was dropped (e.g. Chroma restarted
                # with a fresh volume) but we still hold a stale cached handle.
                # Drop the cache entry, re-create the collection, and retry once.
                self._collections.pop(collection_name or self._collection_name(workspace_id), None)
                collection = self._collection_for(workspace_id, collection_name)
                collection.upsert(ids=ids, documents=documents, metadatas=metadatas)

    def ensure_embedding_model(
        self,
        workspace_id: str,
        embedding_model: str | None,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
    ) -> None:
        provider_changed = api_key != self._embedding_api_key or base_url != self._embedding_base_url
        self._embedding_api_key = api_key
        self._embedding_base_url = base_url
        if not embedding_model or embedding_model == self.active_embedding_model():
            if provider_changed:
                self._embedding_function = None
                self._collections.pop(self._collection_name(workspace_id), None)
            return
        name = self._collection_name(workspace_id)
        self.settings.embedding_model = embedding_model
        self._embedding_function = None
        self._collections.pop(name, None)
        self._test_double.pop(name, None)
        try:
            client = self._get_client()
            client.delete_collection(name)
        except Exception as exc:
            logger.warning(
                "embedding_collection_reset_failed",
                extra={"_collection": name, "_error": exc.__class__.__name__},
            )

    def _clean_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        for key, value in metadata.items():
            if value is None:
                continue
            if isinstance(value, list) and not value:
                continue
            cleaned[key] = value
        return cleaned

    async def delete_ids(self, workspace_id: str, ids: list[str]) -> None:
        if not ids:
            return
        collection = self._get_collection(workspace_id)
        if collection is None and self._use_test_double():
            bucket = self._test_double.get(self._collection_name(workspace_id), {})
            for item_id in ids:
                bucket.pop(item_id, None)
            return
        collection.delete(ids=ids)

    async def search(
        self,
        workspace_id: str,
        query: str,
        top_k: int = 12,
        *,
        source_types: list[str] | None = None,
        connector_slugs: list[str] | None = None,
        asset_ids: list[str] | None = None,
        collection_name: str | None = None,
    ) -> list[VectorResult]:
        collection = self._collection_for(workspace_id, collection_name)
        if collection is None and self._use_test_double():
            bucket = self._test_double.get(collection_name or self._collection_name(workspace_id), {})
            needle = query.lower()
            ranked = sorted(
                bucket.items(),
                key=lambda item: 0 if needle in item[1][0].lower() else 1,
            )
            return [
                VectorResult(id=item_id, document=doc, metadata=metadata)
                for item_id, (doc, metadata) in ranked
                if self._metadata_matches(
                    metadata,
                    source_types=source_types,
                    connector_slugs=connector_slugs,
                    asset_ids=asset_ids,
                )
            ][:top_k]
        where = self._where_filter(source_types=source_types, connector_slugs=connector_slugs, asset_ids=asset_ids)
        kwargs = {"query_texts": [query], "n_results": top_k}
        if where:
            kwargs["where"] = where
        try:
            result = collection.query(**kwargs)
        except Exception as exc:
            if exc.__class__.__name__ == "NotFoundError":
                name = collection_name or self._collection_name(workspace_id)
                self._collections.pop(name, None)
                logger.warning("vector_store_collection_missing", extra={"_collection": name})
                return []
            raise
        ids = result.get("ids", [[]])[0]
        docs = result.get("documents", [[]])[0]
        metas = result.get("metadatas", [[]])[0]
        distances = result.get("distances", [[]])[0] if result.get("distances") else [None] * len(ids)
        return [
            VectorResult(id=item_id, document=doc, metadata=metadata or {}, distance=distance)
            for item_id, doc, metadata, distance in zip(ids, docs, metas, distances, strict=False)
        ]

    def _where_filter(
        self,
        *,
        source_types: list[str] | None = None,
        connector_slugs: list[str] | None = None,
        asset_ids: list[str] | None = None,
    ) -> dict[str, Any] | None:
        clauses: list[dict[str, Any]] = []
        if source_types:
            clauses.append({"source_type": {"$in": source_types}})
        if connector_slugs:
            clauses.append({"connector_slug": {"$in": connector_slugs}})
        if asset_ids:
            clauses.append({"asset_id": {"$in": asset_ids}})
        if not clauses:
            return None
        if len(clauses) == 1:
            return clauses[0]
        return {"$and": clauses}

    def _metadata_matches(
        self,
        metadata: dict[str, Any],
        *,
        source_types: list[str] | None,
        connector_slugs: list[str] | None,
        asset_ids: list[str] | None = None,
    ) -> bool:
        if source_types and metadata.get("source_type") not in source_types:
            return False
        if connector_slugs and metadata.get("connector_slug") not in connector_slugs:
            return False
        if asset_ids and metadata.get("asset_id") not in asset_ids:
            return False
        return True

    async def search_brain_summaries(
        self,
        workspace_id: str,
        query: str,
        top_k: int = 20,
        *,
        connector_slugs: list[str] | None = None,
    ) -> list[VectorResult]:
        return await self.search(
            workspace_id,
            query,
            top_k=top_k,
            connector_slugs=connector_slugs,
            collection_name=self._brain_collection_name(workspace_id),
        )


vector_store = VectorStore()
