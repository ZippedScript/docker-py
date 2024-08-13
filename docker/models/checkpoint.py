from __future__ import annotations

import tarfile
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

from .containers import Container
from .resource import Collection, Model

if TYPE_CHECKING:
    from ..client import DockerClient


class Checkpoint(Model):
    client: DockerClient
    id_attribute = "Id"

    def __init__(
        self,
        container: Container,
        attrs: dict,
        client: DockerClient,
        collection: CheckpointCollection,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)
        self._container = container

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.short_id} [{self.container.id}]>"

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.id == other.id
            and self.container.id == other.container.id
        )

    def __hash__(self):
        return hash(f"{self.__class__.__name__}:{self.id}:{self.container.id}")

    @property
    def container(self) -> Container:
        return self._container

    @property
    def short_id(self):
        return self.id

    @property
    def tar(self) -> bytes:
        bio = BytesIO()
        if not hasattr(self, "_tar"):
            with tarfile.open(fileobj=bio, mode="w:gz") as tar:
                tar.add(self.dir, arcname=".")
            self._tar = bio.getvalue()
        return self._tar

    @property
    def tar_stream(self) -> BytesIO:
        return BytesIO(self.tar)

    @property
    def dir(self) -> Path:
        resolve_p = Path(self.container.attrs["ResolvConfPath"])
        checkpoint_dir = resolve_p.parent / "checkpoints" / self.id
        if not checkpoint_dir.exists():
            raise FileNotFoundError(
                f"No checkpoint directory found at {checkpoint_dir}"
            )
        return checkpoint_dir

    @property
    def sha256(self) -> str:
        return sha256(self.tar).hexdigest()

    def delete(self):
        self.client.api.delete_checkpoint(self.container.id, self.id)

    def reload(self):
        self.container.reload()
        self.attrs = self.collection.get(self.id, self.container.id).attrs

    def start(self):
        self.client.api.start_checkpoint(self.container.id, self.id)


class CheckpointCollection(Collection):
    model = Checkpoint
    client: DockerClient

    def create(
        self,
        container: Container | str,
        checkpoint_id: str,
    ) -> Checkpoint:  # type: ignore

        if isinstance(container, str):
            container: Container = self.client.containers.get(container)
        self.client.api.create_checkpoint(container.id, checkpoint_id)
        return self.model(
            container, {"Id": checkpoint_id}, client=self.client, collection=self
        )

    def get(self, id: str, container_id: str) -> Checkpoint:
        return self.model(
            self.client.containers.get(container_id),
            {"Id": id},
            client=self.client,
            collection=self,
        )

    def list(self, container: str | Container) -> list[Checkpoint]:
        if isinstance(container, str):
            container = self.client.containers.get(container)
        res = self.client.api.checkpoints(container.id)
        return [self.create(container, c["Name"]) for c in res]
