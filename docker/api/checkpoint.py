from __future__ import annotations

from typing import TYPE_CHECKING

from ..errors import CheckpointExists

if TYPE_CHECKING:
    from .client import APIClient

    class Base(APIClient):
        pass

else:

    class Base:
        pass


class CheckpointApiMixin(Base):

    def create_checkpoint(self, container: str, checkpoint_id: str):
        url = self._url("/containers/{0}/checkpoints", container)
        data = {"checkpointId": checkpoint_id}
        res = self._post_json(url, data=data)
        if not res.status_code == 500:
            if (
                f"checkpoint with name {checkpoint_id} already exists for container {container}"
                in res.text
            ):
                raise CheckpointExists(checkpoint_id, container)
        return self._result(res, False)

    def delete_checkpoint(self, container: str, checkpoint_id: str):
        url = self._url("/containers/{0}/checkpoints/{1}", container, checkpoint_id)
        res = self._delete(url)
        return self._result(res, False)

    def checkpoints(self, container: str):
        url = self._url("/containers/{0}/checkpoints", container)
        res = self._get(url)
        return self._result(res, True)

    def start_checkpoint(self, container: str, checkpoint_id: str):
        url = self._url("/containers/{0}/start", container)

        data = {"checkpoint": checkpoint_id}

        res = self._post_json(url, data=data)
        self._raise_for_status(res)
